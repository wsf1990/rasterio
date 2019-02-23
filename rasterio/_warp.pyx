# distutils: language = c++
"""Raster and vector warping and reprojection."""

include "gdal.pxi"

import logging
import uuid
import warnings
import xml.etree.ElementTree as ET

from affine import identity
import numpy as np

import rasterio
from rasterio._base import gdal_version
from rasterio._err import (
    CPLE_BaseError, CPLE_IllegalArgError, CPLE_NotSupportedError,
    CPLE_AppDefinedError, CPLE_OpenFailedError)
from rasterio import dtypes
from rasterio.control import GroundControlPoint
from rasterio.enums import Resampling, MaskFlags, ColorInterp
from rasterio.env import GDALVersion
from rasterio.crs import CRS
from rasterio.errors import (
    GDALOptionNotImplementedError,
    DriverRegistrationError, CRSError, RasterioIOError,
    RasterioDeprecationWarning, WarpOptionsError)
from rasterio.transform import Affine, from_bounds, guard_transform, tastes_like_gdal

cimport numpy as np

from rasterio._base cimport _osr_from_crs, get_driver_name, _safe_osr_release
from rasterio._err cimport exc_wrap_pointer, exc_wrap_int
from rasterio._io cimport (
    DatasetReaderBase, InMemoryRaster, in_dtype_range, io_auto)
from rasterio._features cimport GeomBuilder, OGRGeomBuilder
from rasterio._shim cimport delete_nodata_value, open_dataset


log = logging.getLogger(__name__)


def recursive_round(val, precision):
    """Recursively round coordinates."""
    if isinstance(val, (int, float)):
        return round(val, precision)
    else:
        return [recursive_round(part, precision) for part in val]


def _transform_geom(
        src_crs, dst_crs, geom, antimeridian_cutting, antimeridian_offset,
        precision):
    """Return a transformed geometry."""
    cdef char **options = NULL
    cdef OGRSpatialReferenceH src = NULL
    cdef OGRSpatialReferenceH dst = NULL
    cdef OGRCoordinateTransformationH transform = NULL
    cdef OGRGeometryFactory *factory = NULL
    cdef OGRGeometryH src_geom = NULL
    cdef OGRGeometryH dst_geom = NULL
    cdef int i

    src = _osr_from_crs(src_crs)
    dst = _osr_from_crs(dst_crs)

    try:
        transform = exc_wrap_pointer(OCTNewCoordinateTransformation(src, dst))
    except:
        _safe_osr_release(src)
        _safe_osr_release(dst)
        raise

    if GDALVersion().runtime() < GDALVersion.parse('2.2'):
        valb = str(antimeridian_offset).encode('utf-8')
        options = CSLSetNameValue(options, "DATELINEOFFSET", <const char *>valb)
        if antimeridian_cutting:
            options = CSLSetNameValue(options, "WRAPDATELINE", "YES")
    else:
        # GDAL cuts on the antimeridian by default and using different
        # logic in versions >= 2.2.
        pass

    try:
        factory = new OGRGeometryFactory()
        src_geom = OGRGeomBuilder().build(geom)
        dst_geom = exc_wrap_pointer(
            factory.transformWithOptions(
                <const OGRGeometry *>src_geom,
                <OGRCoordinateTransformation *>transform,
                options))

        result = GeomBuilder().build(dst_geom)

        if precision >= 0:
            # TODO: Geometry collections.
            result['coordinates'] = recursive_round(result['coordinates'],
                                                    precision)

        return result

    finally:
        del factory
        OGR_G_DestroyGeometry(dst_geom)
        OGR_G_DestroyGeometry(src_geom)
        OCTDestroyCoordinateTransformation(transform)
        if options != NULL:
            CSLDestroy(options)
        _safe_osr_release(src)
        _safe_osr_release(dst)


cdef GDALWarpOptions * create_warp_options(
        GDALResampleAlg resampling, object src_nodata, object dst_nodata, int src_count,
        object dst_alpha, object src_alpha, int warp_mem_limit, const char **options) except NULL:
    """Return a pointer to a GDALWarpOptions composed from input params

    This is used in _reproject() and the WarpedVRT constructor. It sets
    up warp options in almost exactly the same way as gdawarp.

    Parameters
    ----------
    dst_alpha : int
        This parameter specifies a destination alpha band for the
        warper.
    src_alpha : int
        This parameter specifies the source alpha band for the warper.

    """

    cdef GDALWarpOptions *psWOptions = GDALCreateWarpOptions()

    # Note: warp_extras is pointed to different memory locations on every
    # call to CSLSetNameValue call below, but needs to be set here to
    # get the defaults.
    cdef char **warp_extras = psWOptions.papszWarpOptions

    # See http://www.gdal.org/structGDALWarpOptions.html#a0ed77f9917bb96c7a9aabd73d4d06e08
    # for a list of supported options. Copying unsupported options
    # is fine.

    # Use the same default nodata logic as gdalwarp.
    warp_extras = CSLSetNameValue(
        warp_extras, "UNIFIED_SRC_NODATA", "YES")

    warp_extras = CSLMerge(warp_extras, <char **>options)

    psWOptions.eResampleAlg = <GDALResampleAlg>resampling

    if warp_mem_limit > 0:
        psWOptions.dfWarpMemoryLimit = <double>warp_mem_limit * 1024 * 1024
        log.debug("Warp Memory Limit set: {!r}".format(warp_mem_limit))

    band_count = src_count

    if src_alpha:
        psWOptions.nSrcAlphaBand = src_alpha

    if dst_alpha:
        psWOptions.nDstAlphaBand = dst_alpha

    # Assign nodata values.
    # We don't currently support an imaginary component.

    if src_nodata is not None:
        psWOptions.padfSrcNoDataReal = <double*>CPLMalloc(band_count * sizeof(double))
        psWOptions.padfSrcNoDataImag = <double*>CPLMalloc(band_count * sizeof(double))

        for i in range(band_count):
            psWOptions.padfSrcNoDataReal[i] = float(src_nodata)
            psWOptions.padfSrcNoDataImag[i] = 0.0

    if dst_nodata is not None:
        psWOptions.padfDstNoDataReal = <double*>CPLMalloc(band_count * sizeof(double))
        psWOptions.padfDstNoDataImag = <double*>CPLMalloc(band_count * sizeof(double))

        for i in range(band_count):
            psWOptions.padfDstNoDataReal[i] = float(dst_nodata)
            psWOptions.padfDstNoDataImag[i] = 0.0


    # Important: set back into struct or values set above are lost
    # This is because CSLSetNameValue returns a new list each time
    psWOptions.papszWarpOptions = warp_extras

    # Set up band info
    if psWOptions.nBandCount == 0:
        psWOptions.nBandCount = band_count

        psWOptions.panSrcBands = <int*>CPLMalloc(band_count * sizeof(int))
        psWOptions.panDstBands = <int*>CPLMalloc(band_count * sizeof(int))

        for i in range(band_count):
            psWOptions.panSrcBands[i] = i + 1
            psWOptions.panDstBands[i] = i + 1

    return psWOptions


def _reproject(
        source, destination,
        src_transform=None,
        gcps=None,
        src_crs=None,
        src_nodata=None,
        dst_transform=None,
        dst_crs=None,
        dst_nodata=None,
        dst_alpha=0,
        src_alpha=0,
        resampling=Resampling.nearest,
        init_dest_nodata=True,
        num_threads=1,
        warp_mem_limit=0,
        **kwargs):
    """
    Reproject a source raster to a destination raster.

    If the source and destination are ndarrays, coordinate reference
    system definitions and affine transformation parameters are required
    for reprojection.

    If the source and destination are rasterio Bands, shorthand for
    bands of datasets on disk, the coordinate reference systems and
    transforms will be read from the appropriate datasets.

    Parameters
    ------------
    source: ndarray or rasterio Band
        Source raster.
    destination: ndarray or rasterio Band
        Target raster.
    src_transform: affine.Affine(), optional
        Source affine transformation.  Required if source and destination
        are ndarrays.  Will be derived from source if it is a rasterio Band.
    gcps: sequence of `GroundControlPoint` instances, optional
        Ground control points for the source. May be used in place of
        src_transform.
    src_crs: dict, optional
        Source coordinate reference system, in rasterio dict format.
        Required if source and destination are ndarrays.
        Will be derived from source if it is a rasterio Band.
        Example: {'init': 'EPSG:4326'}
    src_nodata: int or float, optional
        The source nodata value.  Pixels with this value will not be used
        for interpolation.  If not set, it will be default to the
        nodata value of the source image if a masked ndarray or rasterio band,
        if available.
    dst_transform: affine.Affine(), optional
        Target affine transformation.  Required if source and destination
        are ndarrays.  Will be derived from target if it is a rasterio Band.
    dst_crs: dict, optional
        Target coordinate reference system.  Required if source and destination
        are ndarrays.  Will be derived from target if it is a rasterio Band.
    dst_nodata: int or float, optional
        The nodata value used to initialize the destination; it will remain
        in all areas not covered by the reprojected source.  Defaults to the
        nodata value of the destination image (if set), the value of
        src_nodata, or 0 (gdal default).
    src_alpha : int, optional
        Index of a band to use as the alpha band when warping.
    dst_alpha : int, optional
        Index of a band to use as the alpha band when warping.
    resampling: int
        Resampling method to use.  One of the following:
            Resampling.nearest,
            Resampling.bilinear,
            Resampling.cubic,
            Resampling.cubic_spline,
            Resampling.lanczos,
            Resampling.average,
            Resampling.mode
    init_dest_nodata: bool
        Flag to specify initialization of nodata in destination;
        prevents overwrite of previous warps. Defaults to True.
    num_threads : int
        Number of worker threads.
    warp_mem_limit : int, optional
        The warp operation memory limit in MB. Larger values allow the
        warp operation to be carried out in fewer chunks. The amount of
        memory required to warp a 3-band uint8 2000 row x 2000 col
        raster to a destination of the same size is approximately
        56 MB. The default (0) means 64 MB with GDAL 2.2.
        The warp operation's memory limit in MB. The default (0)
        means 64 MB with GDAL 2.2.
    kwargs:  dict, optional
        Additional arguments passed to both the image to image
        transformer GDALCreateGenImgProjTransformer2() (for example,
        MAX_GCP_ORDER=2) and to the Warper (for example,
        INIT_DEST=NO_DATA).

    Returns
    ---------
    out: None
        Output is written to destination.
    """
    cdef int src_count
    cdef GDALDatasetH src_dataset = NULL
    cdef GDALDatasetH dst_dataset = NULL
    cdef char **warp_extras = NULL
    cdef const char* pszWarpThread = NULL
    cdef int i
    cdef double tolerance = 0.125
    cdef void *hTransformArg = NULL
    cdef GDALTransformerFunc pfnTransformer = NULL
    cdef GDALWarpOptions *psWOptions = NULL

    # Validate nodata values immediately.
    if src_nodata is not None:
        if not in_dtype_range(src_nodata, source.dtype):
            raise ValueError("src_nodata must be in valid range for "
                             "source dtype")

    if dst_nodata is not None:
        if not in_dtype_range(dst_nodata, destination.dtype):
            raise ValueError("dst_nodata must be in valid range for "
                             "destination dtype")
        
    def format_transform(in_transform):
        if not in_transform:
            return in_transform
        in_transform = guard_transform(in_transform)
        # If working with identity transform, assume it is crs-less data
        # and that translating the matrix very slightly will avoid #674 and #1272
        eps = 1e-100
        if in_transform.almost_equals(identity) or in_transform.almost_equals(Affine(1, 0, 0, 0, -1, 0)):
            in_transform = in_transform.translation(eps, eps)
        return in_transform

    # If the source is an ndarray, we copy to a MEM dataset.
    # We need a src_transform and src_dst in this case. These will
    # be copied to the MEM dataset.
    if dtypes.is_ndarray(source):
        if not src_crs:
            raise CRSError("Missing src_crs.")
        if src_nodata is None and hasattr(source, 'fill_value'):
            # source is a masked array
            src_nodata = source.fill_value
        # Convert 2D single-band arrays to 3D multi-band.
        if len(source.shape) == 2:
            source = source.reshape(1, *source.shape)
        src_count = source.shape[0]
        src_bidx = range(1, src_count + 1)
        src_dataset = InMemoryRaster(image=source,
                                     transform=format_transform(src_transform),
                                     gcps=gcps,
                                     crs=src_crs).handle()
    # If the source is a rasterio MultiBand, no copy necessary.
    # A MultiBand is a tuple: (dataset, bidx, dtype, shape(2d))
    elif isinstance(source, tuple):
        rdr, src_bidx, dtype, shape = source
        if isinstance(src_bidx, int):
            src_bidx = [src_bidx]
        src_count = len(src_bidx)
        src_dataset = (<DatasetReaderBase?>rdr).handle()
        if src_nodata is None:
            src_nodata = rdr.nodata
    else:
        raise ValueError("Invalid source")

    # Next, do the same for the destination raster.
    if dtypes.is_ndarray(destination):
        if not dst_crs:
            raise CRSError("Missing dst_crs.")
        if len(destination.shape) == 2:
            destination = destination.reshape(1, *destination.shape)

        if destination.shape[0] == src_count:
            # Output shape matches number of bands being extracted
            dst_bidx = [i + 1 for i in range(src_count)]
        else:
            # Assume src and dst are the same shape
            if max(src_bidx) > destination.shape[0]:
                raise ValueError("Invalid destination shape")
            dst_bidx = src_bidx

        dst_dataset = InMemoryRaster(image=destination,
                                     transform=format_transform(dst_transform),
                                     crs=dst_crs).handle()
        if dst_alpha:
            for i in range(destination.shape[0]):
                try:
                    delete_nodata_value(GDALGetRasterBand(dst_dataset, i+1))
                except NotImplementedError as exc:
                    log.warn(str(exc))

            GDALSetRasterColorInterpretation(GDALGetRasterBand(dst_dataset, dst_alpha), <GDALColorInterp>6)

        GDALSetDescription(
            dst_dataset, "Temporary destination dataset for _reproject()")

        log.debug("Created temp destination dataset.")

        if dst_nodata is None:
            if hasattr(destination, "fill_value"):
                # destination is a masked array
                dst_nodata = destination.fill_value
            elif src_nodata is not None:
                dst_nodata = src_nodata

    elif isinstance(destination, tuple):
        udr, dst_bidx, _, _ = destination
        if isinstance(dst_bidx, int):
            dst_bidx = [dst_bidx]
        dst_dataset = (<DatasetReaderBase?>udr).handle()
        if dst_nodata is None:
            dst_nodata = udr.nodata
    else:
        raise ValueError("Invalid destination")

    # Set up GDALCreateGenImgProjTransformer2 keyword arguments.
    cdef char **imgProjOptions = NULL
    CSLSetNameValue(imgProjOptions, "GCPS_OK", "TRUE")

    # See http://www.gdal.org/gdal__alg_8h.html#a94cd172f78dbc41d6f407d662914f2e3
    # for a list of supported options. I (Sean) don't see harm in
    # copying all the function's keyword arguments to the image to
    # image transformer options mapping; unsupported options should be
    # okay.
    for key, val in kwargs.items():
        key = key.upper().encode('utf-8')
        val = str(val).upper().encode('utf-8')
        imgProjOptions = CSLSetNameValue(
            imgProjOptions, <const char *>key, <const char *>val)

    try:
        hTransformArg = exc_wrap_pointer(
            GDALCreateGenImgProjTransformer2(
                src_dataset, dst_dataset, imgProjOptions))
        hTransformArg = exc_wrap_pointer(
            GDALCreateApproxTransformer(
                GDALGenImgProjTransform, hTransformArg, tolerance))
        pfnTransformer = GDALApproxTransform
        GDALApproxTransformerOwnsSubtransformer(hTransformArg, 1)

        log.debug("Created transformer and options.")

    except:
        GDALDestroyApproxTransformer(hTransformArg)
        CPLFree(imgProjOptions)
        raise

    valb = str(num_threads).encode('utf-8')
    warp_extras = CSLSetNameValue(warp_extras, "NUM_THREADS", <const char *>valb)

    log.debug("Setting NUM_THREADS option: %d", num_threads)

    if init_dest_nodata:
        warp_extras = CSLSetNameValue(warp_extras, "INIT_DEST", "NO_DATA")

    # See http://www.gdal.org/structGDALWarpOptions.html#a0ed77f9917bb96c7a9aabd73d4d06e08
    # for a list of supported options. Copying unsupported options
    # is fine.
    for key, val in kwargs.items():
        key = key.upper().encode('utf-8')
        val = str(val).upper().encode('utf-8')
        warp_extras = CSLSetNameValue(
            warp_extras, <const char *>key, <const char *>val)

    cdef GDALRasterBandH hBand = NULL

    psWOptions = create_warp_options(
        <GDALResampleAlg>resampling, src_nodata,
        dst_nodata, src_count, dst_alpha, src_alpha, warp_mem_limit,
        <const char **>warp_extras)

    psWOptions.pfnTransformer = pfnTransformer
    psWOptions.pTransformerArg = hTransformArg
    psWOptions.hSrcDS = src_dataset
    psWOptions.hDstDS = dst_dataset

    for idx, (s, d) in enumerate(zip(src_bidx, dst_bidx)):
        psWOptions.panSrcBands[idx] = s
        psWOptions.panDstBands[idx] = d
        log.debug('Configured to warp src band %d to destination band %d' % (s, d))

    log.debug("Set transformer options")

    # Now that the transformer and warp options are set up, we init
    # and run the warper.
    cdef GDALWarpOperation oWarper
    cdef int rows
    cdef int cols
    try:
        exc_wrap_int(oWarper.Initialize(psWOptions))
        rows, cols = destination.shape[-2:]

        log.debug(
            "Chunk and warp window: %d, %d, %d, %d.",
            0, 0, cols, rows)

        if num_threads > 1:
            with nogil:
                oWarper.ChunkAndWarpMulti(0, 0, cols, rows)
        else:
            with nogil:
                oWarper.ChunkAndWarpImage(0, 0, cols, rows)

        if dtypes.is_ndarray(destination):
            exc_wrap_int(io_auto(destination, dst_dataset, 0))

            if dst_dataset != NULL:
                GDALClose(dst_dataset)

    # Clean up transformer, warp options, and dataset handles.
    finally:
        GDALDestroyApproxTransformer(hTransformArg)
        GDALDestroyWarpOptions(psWOptions)
        CPLFree(imgProjOptions)
        if dtypes.is_ndarray(source):
            if src_dataset != NULL:
                GDALClose(src_dataset)


def _calculate_default_transform(src_crs, dst_crs, width, height,
                                 left=None, bottom=None, right=None, top=None,
                                 gcps=None, **kwargs):
    """Wraps GDAL's algorithm."""
    cdef void *hTransformArg = NULL
    cdef int npixels = 0
    cdef int nlines = 0
    cdef double extent[4]
    cdef double geotransform[6]
    cdef OGRSpatialReferenceH osr = NULL
    cdef char *wkt = NULL
    cdef GDALDatasetH hds = NULL

    extent[:] = [0.0, 0.0, 0.0, 0.0]
    geotransform[:] = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

    # Make an in-memory raster dataset we can pass to
    # GDALCreateGenImgProjTransformer().

    if all(x is not None for x in (left, bottom, right, top)):
        transform = from_bounds(left, bottom, right, top, width, height)
    elif any(x is not None for x in (left, bottom, right, top)):
        raise ValueError(
            "Some, but not all, bounding box parameters were provided.")
    else:
        transform = None

    try:
        osr = _osr_from_crs(dst_crs)
        exc_wrap_int(OSRExportToWkt(osr, &wkt))
    except CPLE_BaseError as exc:
        raise CRSError("Could not convert to WKT. {}".format(str(exc)))
    finally:
        _safe_osr_release(osr)

    if isinstance(src_crs, str):
        src_crs = CRS.from_string(src_crs)
    elif isinstance(src_crs, dict):
        src_crs = CRS(**src_crs)

    vrt_doc = _suggested_proxy_vrt_doc(width, height, transform=transform, crs=src_crs, gcps=gcps).decode('ascii')

    try:
        try:
            hds = open_dataset(vrt_doc, 0x00 | 0x02 | 0x04, ['VRT'], {}, None)
        except GDALOptionNotImplementedError:
            hds = open_dataset(vrt_doc, 0x00 | 0x02 | 0x04, None, None, None)

        hTransformArg = exc_wrap_pointer(
            GDALCreateGenImgProjTransformer(
                hds, NULL, NULL, wkt, 1, 1000.0,0))
        exc_wrap_int(
            GDALSuggestedWarpOutput2(
                hds, GDALGenImgProjTransform, hTransformArg,
                geotransform, &npixels, &nlines, extent, 0))

        log.debug("Created transformer and warp output.")

    except CPLE_NotSupportedError as err:
        raise CRSError(err.errmsg)

    except CPLE_AppDefinedError as err:
        if "Reprojection failed" in str(err):
            # This "exception" should be treated as a debug msg, not error
            # "Reprojection failed, err = -14, further errors will be
            # suppressed on the transform object."
            log.info("Encountered points outside of valid dst crs region")
            pass
        else:
            raise err
    finally:
        if wkt != NULL:
            CPLFree(wkt)
        if hTransformArg != NULL:
            GDALDestroyGenImgProjTransformer(hTransformArg)

    # Convert those modified arguments to Python values.
    dst_affine = Affine.from_gdal(*[geotransform[i] for i in range(6)])
    dst_width = npixels
    dst_height = nlines

    return dst_affine, dst_width, dst_height


DEFAULT_NODATA_FLAG = object()


cdef class WarpedVRTReaderBase(DatasetReaderBase):

    def __init__(self, src_dataset, src_crs=None, dst_crs=None, crs=None,
                 resampling=Resampling.nearest, tolerance=0.125,
                 src_nodata=DEFAULT_NODATA_FLAG, dst_nodata=None, nodata=DEFAULT_NODATA_FLAG,
                 dst_width=None, width=None, dst_height=None, height=None,
                 src_transform=None, dst_transform=None, transform=None,
                 init_dest_nodata=True, src_alpha=0, add_alpha=False,
                 warp_mem_limit=0, **warp_extras):
        """Make a virtual warped dataset

        Parameters
        ----------
        src_dataset : dataset object
            The warp source.
        src_crs : CRS or str, optional
            Overrides the coordinate reference system of `src_dataset`.
        src_transfrom : Affine, optional
            Overrides the transform of `src_dataset`.
        src_nodata : float, optional
            Overrides the nodata value of `src_dataset`, which is the
            default.
        crs : CRS or str, optional
            The coordinate reference system at the end of the warp
            operation.  Default: the crs of `src_dataset`. dst_crs is
            a deprecated alias for this parameter.
        transform : Affine, optional
            The transform for the virtual dataset. Default: will be
            computed from the attributes of `src_dataset`. dst_transform
            is a deprecated alias for this parameter.
        height, width: int, optional
            The dimensions of the virtual dataset. Defaults: will be
            computed from the attributes of `src_dataset`. dst_height
            and dst_width are deprecated alias for these parameters.
        nodata : float, optional
            Nodata value for the virtual dataset. Default: the nodata
            value of `src_dataset` or 0.0. dst_nodata is a deprecated
            alias for this parameter.
        resampling : Resampling, optional
            Warp resampling algorithm. Default: `Resampling.nearest`.
        tolerance : float, optional
            The maximum error tolerance in input pixels when
            approximating the warp transformation. Default: 0.125,
            or one-eigth of a pixel.
        src_alpha : int, optional
            Index of a source band to use as an alpha band for warping.
        add_alpha : bool, optional
            Whether to add an alpha masking band to the virtual dataset.
            Default: False. This option will cause deletion of the VRT
            nodata value.
        init_dest_nodata : bool, optional
            Whether or not to initialize output to `nodata`. Default:
            True.
        warp_mem_limit : int, optional
            The warp operation's memory limit in MB. The default (0)
            means 64 MB with GDAL 2.2.
        warp_extras : dict
            GDAL extra warp options. See
            http://www.gdal.org/structGDALWarpOptions.html.

        Returns
        -------
        WarpedVRT
        """
        self.mode = 'r'
        self.options = {}
        self._count = 0
        self._closed = True
        self._dtypes = []
        self._block_shapes = None
        self._nodatavals = []
        self._units = ()
        self._descriptions = ()
        self._crs = None
        self._gcps = None
        self._read = False

        # The various `dst_*` parameters are deprecated and will be
        # removed in 1.1. In the next section of code, we warn
        # about the deprecation and treat `dst_parameter` as an
        # alias for `parameter`.

        # Deprecate dst_nodata.
        if dst_nodata is not None:
            warnings.warn(
                "dst_nodata will be removed in 1.1, use nodata",
                RasterioDeprecationWarning)
        if nodata is None:
            nodata = dst_nodata

        # Deprecate dst_width.
        if dst_width is not None:
            warnings.warn(
                "dst_width will be removed in 1.1, use width",
                RasterioDeprecationWarning)
        if width is None:
            width = dst_width

        # Deprecate dst_height.
        if dst_height is not None:
            warnings.warn(
                "dst_height will be removed in 1.1, use height",
                RasterioDeprecationWarning)
        if height is None:
            height = dst_height

        # Deprecate dst_transform.
        if dst_transform is not None:
            warnings.warn(
                "dst_transform will be removed in 1.1, use transform",
                RasterioDeprecationWarning)
        if transform is None:
            transform = dst_transform

        # Deprecate dst_crs.
        if dst_crs is not None:
            warnings.warn(
                "dst_crs will be removed in 1.1, use crs",
                RasterioDeprecationWarning)
        if crs is None:
            crs = dst_crs if dst_crs is not None else src_dataset.crs
        # End of `dst_parameter` deprecation and aliasing.

        if add_alpha and gdal_version().startswith('1'):
            warnings.warn("Alpha addition not supported by GDAL 1.x")
            add_alpha = False

        # kwargs become warp options.
        self.src_dataset = src_dataset
        self.src_crs = CRS.from_user_input(src_crs) if src_crs else None
        self.dst_crs = CRS.from_user_input(crs) if crs else None
        self.src_transform = src_transform
        self.name = "WarpedVRT({})".format(src_dataset.name)
        self.resampling = resampling
        self.tolerance = tolerance

        self.src_nodata = self.src_dataset.nodata if src_nodata is DEFAULT_NODATA_FLAG else src_nodata
        self.dst_nodata = self.src_nodata if nodata is DEFAULT_NODATA_FLAG else nodata
        self.dst_width = width
        self.dst_height = height
        self.dst_transform = transform
        self.warp_extras = warp_extras.copy()
        if init_dest_nodata is True and 'init_dest' not in warp_extras:
            self.warp_extras['init_dest'] = 'NO_DATA'

        cdef GDALDriverH driver = NULL
        cdef GDALDatasetH hds = NULL
        cdef GDALDatasetH hds_warped = NULL
        cdef const char *cypath = NULL
        cdef char *src_crs_wkt = NULL
        cdef char *dst_crs_wkt = NULL
        cdef OGRSpatialReferenceH osr = NULL
        cdef char **c_warp_extras = NULL
        cdef GDALWarpOptions *psWOptions = NULL
        cdef float c_tolerance = tolerance
        cdef GDALResampleAlg c_resampling = resampling
        cdef int c_width = self.dst_width or 0
        cdef int c_height = self.dst_height or 0
        cdef double src_gt[6]
        cdef double dst_gt[6]
        cdef void *hTransformArg = NULL
        cdef GDALRasterBandH hband = NULL
        cdef GDALRasterBandH hmask = NULL
        cdef int mask_block_xsize = 0
        cdef int mask_block_ysize = 0

        hds = (<DatasetReaderBase?>self.src_dataset).handle()
        hds = exc_wrap_pointer(hds)

        if not self.src_transform:
            self.src_transform = self.src_dataset.transform

        if self.dst_transform:
            t = self.src_transform.to_gdal()
            for i in range(6):
                src_gt[i] = t[i]

            t = self.dst_transform.to_gdal()
            for i in range(6):
                dst_gt[i] = t[i]

        if not self.src_crs:
            self.src_crs = self.src_dataset.crs

        # Convert CRSes to C WKT strings.
        try:
            osr = _osr_from_crs(self.src_crs)
            OSRExportToWkt(osr, &src_crs_wkt)
        finally:
            if osr != NULL:
                OSRRelease(osr)
            osr = NULL

        if self.dst_crs is not None:
            try:
                osr = _osr_from_crs(self.dst_crs)
                OSRExportToWkt(osr, &dst_crs_wkt)
            finally:
                _safe_osr_release(osr)

        log.debug("Exported CRS to WKT.")

        log.debug("Warp_extras: %r", self.warp_extras)

        for key, val in self.warp_extras.items():
            key = key.upper().encode('utf-8')
            val = str(val).upper().encode('utf-8')
            c_warp_extras = CSLSetNameValue(
                c_warp_extras, <const char *>key, <const char *>val)

        cdef GDALRasterBandH hBand = NULL
        src_alpha_band = 0
        for bidx in src_dataset.indexes:
            hBand = GDALGetRasterBand(hds, bidx)
            if GDALGetRasterColorInterpretation(hBand) == GCI_AlphaBand:
                src_alpha_band = bidx

        if add_alpha:

            # Adding an alpha band when the source has one is trouble.
            # It will result in suprisingly unmasked data. We will 
            # raise an exception instead.
            if src_alpha_band:
                raise WarpOptionsError(
                    "The VRT already has an alpha band, adding a new one is not supported")

            else:
                dst_alpha = src_dataset.count + 1
                self.dst_nodata = None

        else:
            dst_alpha = 0

        if src_alpha:
            src_alpha_band = src_alpha

        psWOptions = create_warp_options(
            <GDALResampleAlg>c_resampling, self.src_nodata,
            self.dst_nodata, src_dataset.count, dst_alpha,
            src_alpha_band, warp_mem_limit, <const char **>c_warp_extras)

        if psWOptions == NULL:
            raise RuntimeError("Warp options are NULL")

        psWOptions.hSrcDS = hds

        try:
            if self.dst_width and self.dst_height and self.dst_transform:
                # set up transform args (otherwise handled in
                # GDALAutoCreateWarpedVRT)
                try:
                    hTransformArg = exc_wrap_pointer(
                        GDALCreateGenImgProjTransformer3(
                            src_crs_wkt, src_gt, dst_crs_wkt, dst_gt))
                    if c_tolerance > 0.0:
                        hTransformArg = exc_wrap_pointer(
                            GDALCreateApproxTransformer(
                                GDALGenImgProjTransform,
                                hTransformArg,
                                c_tolerance))

                        psWOptions.pfnTransformer = GDALApproxTransform

                        GDALApproxTransformerOwnsSubtransformer(
                            hTransformArg, 1)

                    log.debug("Created transformer and options.")
                    psWOptions.pTransformerArg = hTransformArg
                except Exception:
                    GDALDestroyApproxTransformer(hTransformArg)
                    raise

                with nogil:
                    hds_warped = GDALCreateWarpedVRT(
                        hds, c_width, c_height, dst_gt, psWOptions)
                    GDALSetProjection(hds_warped, dst_crs_wkt)
                self._hds = exc_wrap_pointer(hds_warped)
            else:
                with nogil:
                    hds_warped = GDALAutoCreateWarpedVRT(
                        hds, NULL, dst_crs_wkt, c_resampling,
                        c_tolerance, psWOptions)
                self._hds = exc_wrap_pointer(hds_warped)

        except CPLE_OpenFailedError as err:
            raise RasterioIOError(err.errmsg)
        finally:
            CPLFree(dst_crs_wkt)
            CSLDestroy(c_warp_extras)
            if psWOptions != NULL:
                GDALDestroyWarpOptions(psWOptions)

        if self.dst_nodata is None:
            for i in self.indexes:
                try:
                    delete_nodata_value(self.band(i))
                except NotImplementedError as exc:
                    log.warn(str(exc))
        else:
            for i in self.indexes:
                GDALSetRasterNoDataValue(self.band(i), self.dst_nodata)

        if dst_alpha:
            GDALSetRasterColorInterpretation(self.band(dst_alpha), <GDALColorInterp>6)

        self._set_attrs_from_dataset_handle()

        # This attribute will be used by read().
        self._nodatavals = [self.dst_nodata for i in self.indexes]

        if dst_alpha and len(self._nodatavals) == 3:
            self._nodatavals[dst_alpha - 1] = None

    @property
    def crs(self):
        """The dataset's coordinate reference system"""
        return self.dst_crs

    def start(self):
        """Starts the VRT's life cycle."""
        log.debug("Dataset %r is started.", self)

    def stop(self):
        """Ends the VRT's life cycle"""
        if self._hds != NULL:
            GDALClose(self._hds)
        self._hds = NULL

    def read(self, indexes=None, out=None, window=None, masked=False,
            out_shape=None, boundless=False, resampling=Resampling.nearest,
            fill_value=None):
        """Read a dataset's raw pixels as an N-d array"""
        if boundless:
            raise ValueError("WarpedVRT does not permit boundless reads")
        else:
            return super(WarpedVRTReaderBase, self).read(indexes=indexes, out=out, window=window, masked=masked, out_shape=out_shape, resampling=resampling, fill_value=fill_value)

    def read_masks(self, indexes=None, out=None, out_shape=None, window=None,
                   boundless=False, resampling=Resampling.nearest):
        """Read raster band masks as a multidimensional array"""
        if boundless:
            raise ValueError("WarpedVRT does not permit boundless reads")
        else:
            return super(WarpedVRTReaderBase, self).read_masks(indexes=indexes, out=out, window=window, out_shape=out_shape, resampling=resampling)


def _suggested_proxy_vrt_doc(width, height, transform=None, crs=None, gcps=None):
    """Make a VRT XML document to serve _calculate_default_transform."""
    vrtdataset = ET.Element('VRTDataset')
    vrtdataset.attrib['rasterYSize'] = str(height)
    vrtdataset.attrib['rasterXSize'] = str(width)
    vrtrasterband = ET.SubElement(vrtdataset, 'VRTRasterBand')

    srs = ET.SubElement(vrtdataset, 'SRS')
    srs.text = crs.wkt if crs else ""

    if gcps:
        gcplist = ET.SubElement(vrtdataset, 'GCPList')
        gcplist.attrib['Projection'] = crs.wkt if crs else ""
        for point in gcps:
            gcp = ET.SubElement(gcplist, 'GCP')
            gcp.attrib['Id'] = str(point.id)
            gcp.attrib['Info'] = str(point.info)
            gcp.attrib['Pixel'] = str(point.col)
            gcp.attrib['Line'] = str(point.row)
            gcp.attrib['X'] = str(point.x)
            gcp.attrib['Y'] = str(point.y)
            gcp.attrib['Z'] = str(point.z)
    else:
        geotransform = ET.SubElement(vrtdataset, 'GeoTransform')
        geotransform.text = ','.join([str(v) for v in transform.to_gdal()])

    return ET.tostring(vrtdataset)
