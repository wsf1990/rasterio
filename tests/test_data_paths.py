"""Tests of GDAL and PROJ data finding"""

import os.path

from click.testing import CliRunner
import pytest

import rasterio
from rasterio._env import GDALDataFinder, PROJDataFinder
from rasterio.rio.main import main_group


@pytest.mark.wheel
def test_gdal_data():
    """Get GDAL data path from a wheel"""
    assert GDALDataFinder().search() == os.path.join(os.path.dirname(rasterio.__file__), 'gdal_data')


@pytest.mark.wheel
def test_proj_data():
    """Get GDAL data path from a wheel"""
    assert PROJDataFinder().search() == os.path.join(os.path.dirname(rasterio.__file__), 'proj_data')


@pytest.mark.wheel
def test_env_gdal_data():
    runner = CliRunner()
    result = runner.invoke(main_group, ['env', '--gdal-data'])
    assert result.exit_code == 0
    assert result.output.strip() == os.path.join(os.path.dirname(rasterio.__file__), 'gdal_data')


@pytest.mark.wheel
def test_env_proj_data():
    runner = CliRunner()
    result = runner.invoke(main_group, ['env', '--proj-data'])
    assert result.exit_code == 0
    assert result.output.strip() == os.path.join(os.path.dirname(rasterio.__file__), 'proj_data')
