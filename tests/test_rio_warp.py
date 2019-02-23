"""Unittests for $ rio warp"""


import logging
import os
import sys

import affine
import numpy as np
import pytest

import rasterio
from rasterio.env import GDALVersion
from rasterio.warp import SUPPORTED_RESAMPLING, GDAL2_RESAMPLING
from rasterio.rio import warp
from rasterio.rio.main import main_group


logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


def test_dst_crs_error(runner, tmpdir):
    """Invalid JSON is a bad parameter."""
    srcname = 'tests/data/RGB.byte.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', '{foo: bar}'])
    assert result.exit_code == 2
    assert 'for dst_crs: CRS appears to be JSON but is not' in result.output


def test_dst_crs_error_2(runner, tmpdir):
    """Invalid PROJ.4 is a bad parameter."""
    srcname = 'tests/data/RGB.byte.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', '{"proj": "foobar"}'])
    assert result.exit_code == 2
    assert 'Invalid value for dst_crs' in result.output


def test_dst_crs_error_epsg(runner, tmpdir):
    """Malformed EPSG string is a bad parameter."""
    srcname = 'tests/data/RGB.byte.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', 'EPSG:'])
    assert result.exit_code == 2
    assert "for dst_crs: Invalid CRS:" in result.output


def test_dst_crs_error_epsg_2(runner, tmpdir):
    """Invalid EPSG code is a bad parameter."""
    srcname = 'tests/data/RGB.byte.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', 'EPSG:0'])
    assert result.exit_code == 2
    assert 'for dst_crs: EPSG codes are positive integers' in result.output


def test_dst_nodata_float_no_src_nodata_err(runner, tmpdir):
    """Valid integer destination nodata dtype"""
    srcname = 'tests/data/float.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-nodata', '0.0'])
    assert result.exit_code == 2
    assert 'src-nodata must be provided because dst-nodata is not None' in result.output


def test_src_nodata_int_ok(runner, tmpdir):
    """Check if input nodata is overridden"""
    srcname = 'tests/data/RGB.byte.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--src-nodata', '1'])
    assert result.exit_code == 0
    with rasterio.open(outputname) as src:
        assert src.meta['nodata'] == 1


def test_dst_nodata_int_ok(runner, tmpdir):
    """Check if input nodata is overridden"""
    srcname = 'tests/data/RGB.byte.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-nodata', '255'])
    assert result.exit_code == 0
    with rasterio.open(outputname) as src:
        assert src.meta['nodata'] == 255


def test_src_nodata_float_ok(runner, tmpdir):
    """Check if input nodata is overridden"""
    srcname = 'tests/data/float.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--src-nodata', '1.5'])
    assert result.exit_code == 0
    with rasterio.open(outputname) as src:
        assert src.meta['nodata'] == 1.5


def test_dst_nodata_float_override_src_ok(runner, tmpdir):
    """Check if srcnodata is overridden"""
    srcname = 'tests/data/float.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--src-nodata', '1.5', '--dst-nodata', '2.5'])
    assert result.exit_code == 0
    with rasterio.open(outputname) as src:
        assert src.meta['nodata'] == 2.5


def test_warp_no_reproject(runner, tmpdir):
    """ When called without parameters, output should be same as source """
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, ['warp', srcname, outputname])
    assert result.exit_code == 0
    assert os.path.exists(outputname)

    with rasterio.open(srcname) as src:
        with rasterio.open(outputname) as output:
            assert output.count == src.count
            assert output.crs == src.crs
            assert output.nodata == src.nodata
            assert np.allclose(output.bounds, src.bounds)
            assert output.transform.almost_equals(src.transform)
            assert np.allclose(output.read(1), src.read(1))


def test_warp_no_reproject_dimensions(runner, tmpdir):
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dimensions', '100', '100'])
    assert result.exit_code == 0
    assert os.path.exists(outputname)

    with rasterio.open(srcname) as src:
        with rasterio.open(outputname) as output:
            assert output.crs == src.crs
            assert output.width == 100
            assert output.height == 100
            assert np.allclose([97.839396, 97.839396],
                               [output.transform.a, -output.transform.e])


def test_warp_no_reproject_res(runner, tmpdir):
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--res', 30])
    assert result.exit_code == 0
    assert os.path.exists(outputname)

    with rasterio.open(srcname) as src:
        with rasterio.open(outputname) as output:
            assert output.crs == src.crs
            assert np.allclose([30, 30], [output.transform.a, -output.transform.e])
            assert output.width == 327
            assert output.height == 327


def test_warp_no_reproject_bounds(runner, tmpdir):
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    out_bounds = [-11850000, 4810000, -11849000, 4812000]
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--bounds'] + out_bounds)
    assert result.exit_code == 0
    assert os.path.exists(outputname)

    with rasterio.open(srcname) as src:
        with rasterio.open(outputname) as output:
            assert output.crs == src.crs
            assert np.allclose(output.bounds, out_bounds)
            assert np.allclose([src.transform.a, src.transform.e],
                               [output.transform.a, output.transform.e])
            assert output.width == 105
            assert output.height == 210


def test_warp_no_reproject_bounds_res(runner, tmpdir):
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    out_bounds = [-11850000, 4810000, -11849000, 4812000]
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--res', 30, '--bounds'] + out_bounds)
    assert result.exit_code == 0
    assert os.path.exists(outputname)

    with rasterio.open(srcname) as src:
        with rasterio.open(outputname) as output:
            assert output.crs == src.crs
            assert np.allclose(output.bounds, out_bounds)
            assert np.allclose([30, 30], [output.transform.a, -output.transform.e])
            assert output.width == 34
            assert output.height == 67

    # dst-bounds should be an alias to bounds
    outputname = str(tmpdir.join('test2.tif'))
    out_bounds = [-11850000, 4810000, -11849000, 4812000]
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--res', 30, '--dst-bounds'] + out_bounds)
    assert result.exit_code == 0
    assert os.path.exists(outputname)
    with rasterio.open(srcname) as src:
        with rasterio.open(outputname) as output:
            assert np.allclose(output.bounds, out_bounds)


def test_warp_no_reproject_src_bounds_dimensions(runner, tmpdir):
    """--src-bounds option works with dimensions"""
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    out_bounds = [-11850000, 4810000, -11849000, 4812000]
    result = runner.invoke(
        main_group, [
            'warp', srcname, outputname, '--dimensions', 9, 14,
            '--src-bounds'] + out_bounds)
    assert result.exit_code == 0
    assert os.path.exists(outputname)

    with rasterio.open(srcname) as src:
        with rasterio.open(outputname) as output:
            assert output.crs == src.crs
            assert np.allclose(output.bounds, out_bounds)
            assert np.allclose([111.111111, 142.857142],
                               [output.transform.a, -output.transform.e])
            assert output.width == 9
            assert output.height == 14


def test_warp_reproject_dst_crs(runner, tmpdir):
    srcname = 'tests/data/RGB.byte.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', 'EPSG:4326'])
    assert result.exit_code == 0
    assert os.path.exists(outputname)

    with rasterio.open(srcname) as src:
        with rasterio.open(outputname) as output:
            assert output.count == src.count
            assert output.crs == {'init': 'epsg:4326'}
            assert output.width == 835
            assert output.height == 696
            assert np.allclose(output.bounds, [
                -78.95864996545055, 23.564787976164418,
                -76.5759177302349, 25.550873767433984])


def test_warp_reproject_dst_crs_proj4(runner, tmpdir):
    proj4 = '+proj=longlat +ellps=WGS84 +datum=WGS84'
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', proj4])
    assert result.exit_code == 0
    assert os.path.exists(outputname)

    with rasterio.open(outputname) as output:
        assert output.crs == {'init': 'epsg:4326'}  # rasterio converts to EPSG


def test_warp_reproject_res(runner, tmpdir):
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', 'EPSG:4326', '--res', 0.01])
    assert result.exit_code == 0
    assert os.path.exists(outputname)

    with rasterio.open(outputname) as output:
        assert output.crs == {'init': 'epsg:4326'}
        assert np.allclose([0.01, 0.01], [output.transform.a, -output.transform.e])
        assert output.width == 9
        assert output.height == 7


def test_warp_reproject_dimensions(runner, tmpdir):
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', 'EPSG:4326',
        '--dimensions', '100', '100'])
    assert result.exit_code == 0
    assert os.path.exists(outputname)

    with rasterio.open(outputname) as output:
        assert output.crs == {'init': 'epsg:4326'}
        assert output.width == 100
        assert output.height == 100
        assert np.allclose([0.0008789062498762235, 0.0006771676143921468],
                           [output.transform.a, -output.transform.e])


def test_warp_reproject_dimensions_invalid_params(runner, tmpdir):
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    bad_params = [
        ['--bounds', '0', '0', '10', '10'],
        ['--res', '10']
    ]

    for param in bad_params:
        result = runner.invoke(warp.warp,
                               [srcname, outputname, '--dst-crs', 'EPSG:4326',
                                '--dimensions', '100', '100'] +
                               param)

        assert result.exit_code == 2
        assert '--dimensions cannot be used with' in result.output


def test_warp_reproject_bounds_no_res(runner, tmpdir):
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    out_bounds = [-11850000, 4810000, -11849000, 4812000]
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', 'EPSG:4326', '--bounds'] +
        out_bounds)
    assert result.exit_code == 2


def test_warp_reproject_multi_bounds_fail(runner, tmpdir):
    """Mixing --bounds and --src-bounds fails."""
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    out_bounds = [-11850000, 4810000, -11849000, 4812000]
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', 'EPSG:4326',
        '--src-bounds'] + out_bounds + ['--bounds'] + out_bounds)
    assert result.exit_code == 2


def test_warp_reproject_bounds_crossup_fail(runner, tmpdir):
    """Crossed-up bounds raises click.BadParameter."""
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    out_bounds = [-11850000, 4810000, -11849000, 4812000]
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', 'EPSG:4326', '--res', 0.001,
        '--bounds'] + out_bounds)
    assert result.exit_code == 2


def test_warp_reproject_src_bounds_res(runner, tmpdir):
    """--src-bounds option works."""
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    out_bounds = [-11850000, 4810000, -11849000, 4812000]
    result = runner.invoke(
        main_group, [
            'warp', srcname, outputname, '--dst-crs', 'EPSG:4326',
            '--res', 0.001, '--src-bounds'] + out_bounds)
    assert result.exit_code == 0
    assert os.path.exists(outputname)

    with rasterio.open(outputname) as output:
        assert output.crs == {'init': 'epsg:4326'}
        assert np.allclose(output.bounds[:],
                           [-106.45036, 39.6138, -106.44136, 39.6278])
        assert np.allclose([0.001, 0.001],
                           [output.transform.a, -output.transform.e])
        assert output.width == 9
        assert output.height == 14


def test_warp_reproject_src_bounds_dimensions(runner, tmpdir):
    """--src-bounds option works with dimensions"""
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    out_bounds = [-11850000, 4810000, -11849000, 4812000]
    result = runner.invoke(
        main_group, [
            'warp', srcname, outputname, '--dst-crs', 'EPSG:4326',
            '--dimensions', 9, 14, '--src-bounds'] + out_bounds)
    assert result.exit_code == 0
    assert os.path.exists(outputname)

    with rasterio.open(outputname) as output:
        assert output.crs == {'init': 'epsg:4326'}
        assert np.allclose(output.bounds[:],
                           [-106.45036, 39.6138, -106.44136, 39.6278])
        assert round(output.transform.a, 4) == 0.001
        assert round(-output.transform.e, 4) == 0.001


def test_warp_reproject_dst_bounds(runner, tmpdir):
    """--bounds option works."""
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    out_bounds = [-106.45036, 39.6138, -106.44136, 39.6278]
    result = runner.invoke(
        main_group, [
            'warp', srcname, outputname, '--dst-crs', 'EPSG:4326',
            '--res', 0.001, '--bounds'] + out_bounds)
    assert result.exit_code == 0
    assert os.path.exists(outputname)

    with rasterio.open(outputname) as output:
        assert output.crs == {'init': 'epsg:4326'}
        assert np.allclose(output.bounds[0::3],
                           [-106.45036, 39.6278])
        assert np.allclose([0.001, 0.001],
                           [output.transform.a, -output.transform.e])

        # XXX: an extra row and column is produced in the dataset
        # because we're using ceil instead of floor internally.
        # Not necessarily a bug, but may change in the future.
        assert np.allclose([output.bounds[2] - 0.001, output.bounds[1] + 0.001],
                           [-106.44136, 39.6138])
        assert output.width == 10
        assert output.height == 15


def test_warp_reproject_like(runner, tmpdir):
    likename = str(tmpdir.join('like.tif'))
    kwargs = {
        "crs": {'init': 'epsg:4326'},
        "transform": affine.Affine(0.001, 0, -106.523,
                                   0, -0.001, 39.6395),
        "count": 1,
        "dtype": rasterio.uint8,
        "driver": "GTiff",
        "width": 10,
        "height": 10,
        "nodata": 0
    }

    with rasterio.open(likename, 'w', **kwargs) as dst:
        data = np.zeros((10, 10), dtype=rasterio.uint8)
        dst.write(data, indexes=1)

    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--like', likename])
    assert result.exit_code == 0
    assert os.path.exists(outputname)

    with rasterio.open(outputname) as output:
        assert output.crs == {'init': 'epsg:4326'}
        assert np.allclose(
            [0.001, 0.001], [output.transform.a, -output.transform.e])
        assert output.width == 10
        assert output.height == 10


def test_warp_reproject_like_invalid_params(runner, tmpdir):
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    bad_params = [
        ['--dimensions', '10', '10'],
        ['--dst-crs', 'EPSG:4326'],
        ['--bounds', '0', '0', '10', '10'],
        ['--res', '10']
    ]

    for param in bad_params:
        result = runner.invoke(warp.warp,
                               [srcname, outputname, '--like', srcname] +
                               param)

        assert result.exit_code == 2
        assert '--like cannot be used with any of' in result.output


def test_warp_reproject_nolostdata(runner, tmpdir):
    srcname = 'tests/data/world.byte.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', 'EPSG:3857'])
    assert result.exit_code == 0
    assert os.path.exists(outputname)

    with rasterio.open(outputname) as output:
        arr = output.read()
        # 50 column swath on the right edge should have some ones (gdalwarped has 7223)
        assert arr[0, :, -50:].sum() > 7000
        assert output.crs.to_epsg() == 3857


def test_warp_dst_crs_empty_string(runner, tmpdir):
    """`$ rio warp --dst-crs ''` used to perform a falsey check that would treat
    `--dst-crs ''` as though `--dst-crs` was not supplied at all.  If the user
    gives any value we should let `rasterio.crs.from_string()` handle the
    validation.
    """

    infile = 'tests/data/RGB.byte.tif'
    outfile = str(tmpdir.mkdir('empty_warp_dst_crs.tif').join('test.tif'))

    result = runner.invoke(main_group, [
        'warp', infile, outfile, '--dst-crs', ''])

    assert result.exit_code != 0
    assert 'empty or invalid' in result.output


def test_warp_badcrs_dimensions(runner, tmpdir):
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', '{"init": "epsg:-1"}',
        '--dimensions', '100', '100'])
    assert result.exit_code == 2
    assert "Invalid value for dst_crs" in result.output


def test_warp_badcrs_src_bounds(runner, tmpdir):
    srcname = 'tests/data/shade.tif'
    outputname = str(tmpdir.join('test.tif'))
    out_bounds = [-11850000, 4810000, -11849000, 4812000]
    result = runner.invoke(
        main_group, [
            'warp', srcname, outputname, '--dst-crs', '{"init": "epsg:-1"}',
            '--res', 0.001, '--src-bounds'] + out_bounds)
    assert result.exit_code == 2
    assert "Invalid value for dst_crs" in result.output


def test_warp_reproject_check_invert_true(runner, tmpdir):
    outputname = str(tmpdir.join('test.tif'))
    output2name = str(tmpdir.join('test2.tif'))
    srcname = 'tests/data/world.rgb.tif'

    # default True
    runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', 'EPSG:3759'])

    # explicit True
    runner.invoke(main_group, [
        'warp', srcname, output2name, '--check-invert-proj',
        '--dst-crs', 'EPSG:3759'])

    with rasterio.open(outputname) as output, rasterio.open(output2name) as output2:
        assert output.shape == output2.shape


def test_warp_reproject_check_invert_false(runner, tmpdir):
    outputname = str(tmpdir.join('test.tif'))
    output2name = str(tmpdir.join('test2.tif'))
    srcname = 'tests/data/world.rgb.tif'

    # default True
    runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', 'EPSG:3759'])

    # explicit False
    runner.invoke(main_group, [
        'warp', srcname, output2name, '--no-check-invert-proj',
        '--dst-crs', 'EPSG:3759'])

    with rasterio.open(outputname) as output, rasterio.open(output2name) as output2:
        assert output.shape != output2.shape


def test_warp_vrt_gcps(runner, tmpdir):
    """A VRT with GCPs can be warped."""
    srcname = 'tests/data/white-gemini-iv.vrt'
    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', srcname, outputname, '--dst-crs', 'epsg:32618'])
    assert result.exit_code == 0

    # All 4 corners have no data.
    with rasterio.open(outputname) as src:
        data = src.read()
        assert not data[:, 0, 0].any()
        assert not data[:, 0, -1].any()
        assert not data[:, -1, -1].any()
        assert not data[:, -1, 0].any()


@pytest.mark.parametrize("method", SUPPORTED_RESAMPLING)
def test_warp_resampling(runner, path_rgb_byte_tif, tmpdir, method):
    """Resampling methods supported by this version of GDAL should run
    successfully"""

    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', path_rgb_byte_tif, outputname,
        '--dst-crs', 'epsg:3857',
        '--resampling', method.name])

    print(result.output)
    assert result.exit_code == 0


@pytest.mark.skipif(
    GDALVersion.runtime().at_least('2.0'),
    reason="Test only applicable to GDAL < 2.0")
@pytest.mark.parametrize("method", GDAL2_RESAMPLING)
def test_warp_resampling_not_yet_supported(
        runner, path_rgb_byte_tif, tmpdir, method):
    """Resampling methods not yet supported should fail with error"""

    outputname = str(tmpdir.join('test.tif'))
    result = runner.invoke(main_group, [
        'warp', path_rgb_byte_tif, outputname,
        '--dst-crs', 'epsg:3857',
        '--resampling', method.name])

    assert result.exit_code == 2
    assert 'Invalid value for "--resampling"' in result.output
