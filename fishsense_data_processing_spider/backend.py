'''Discovery backend
'''
import datetime as dt
import json
import logging
import multiprocessing
import multiprocessing.pool
from hashlib import md5
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import exiftool
import numpy as np
from label_studio_sdk.client import LabelStudio
from PIL import ExifTags, Image

from fishsense_data_processing_spider.config import settings


def get_image_date(path: Path) -> Optional[dt.datetime]:
    """Retrieves the date of the specified image

    Args:
        path (Path): Path to image

    Returns:
        Optional[dt.datetime]: Image date, otherwise None if unable to open image
    """
    try:
        img = Image.open(path)
        exif = img.getexif()
        creation_time_str = exif.get(ExifTags.Base.DateTime)
        creation_time = dt.datetime.strptime(
            creation_time_str, '%Y:%m:%d %H:%M:%S')
        return creation_time
    except Exception:  # pylint: disable=broad-exception-caught
        return None


def get_dive_date(path: Path) -> Tuple[dt.date, bool, bool]:
    """Computes the date of the dive

    Args:
        path (Path): Path to dive directory

    Returns:
        Tuple[dt.date, bool, bool]: Tuple of nominal dive date, flag indicating whether any date
        data is invalid, and flag indicating whether the dates of images span multiple days
    """
    jpgs = path.glob('*.JPG')
    with multiprocessing.Pool() as pool:
        img_dates = pool.map(get_image_date, jpgs)
    invalid_dates = any(date is None for date in img_dates)
    valid_dates = [date for date in img_dates if date]
    timestamps = [date.timestamp() for date in valid_dates]
    if len(timestamps) == 0:
        return None, False, False
    end_time = dt.datetime.fromtimestamp(max(timestamps))
    start_time = dt.datetime.fromtimestamp(min(timestamps))
    multiple_dates = (end_time.date() != start_time.date())
    mean_date = dt.datetime.fromtimestamp(np.mean(timestamps)).date()
    return mean_date, invalid_dates, multiple_dates


def get_file_checksum(path: Path) -> str:
    """Computes the checksum for the file

    Args:
        path (Path): Path to file to checksum

    Returns:
        str: Checksum of file
    """
    cksum = md5()
    with open(path, 'rb') as handle:
        for blob in iter(lambda: handle.read(8192), b''):
            cksum.update(blob)
    return cksum.hexdigest()


def get_dive_checksum(path: Path) -> str:
    """Computes the checksum for the dive

    Args:
        path (Path): Path to dive directory

    Returns:
        str: Dive checksum
    """
    reference_data = sorted(path.glob('*.ORF'))
    with multiprocessing.Pool() as pool:
        checksums = pool.map(get_file_checksum, reference_data)
    cksum = md5()
    for idx, file in enumerate(reference_data):
        cksum.update(f'{file.name}:{checksums[idx]}\n'.encode())
    return cksum.hexdigest()


def get_olympus_camera_sn(path: Path) -> Optional[str]:
    """Retrieves the camera serial number

    Args:
        path (Path): Path to image

    Returns:
        Optional[str]: Camera Serial Number
    """
    __log = logging.getLogger('exiftool')
    try:
        with exiftool.ExifToolHelper(executable=settings.exiftool.path.as_posix()) as et:
            metadata = et.get_tags(
                path.as_posix(), ['MakerNotes:SerialNumber'])[0]
        return metadata['MakerNotes:SerialNumber'].strip()
    except Exception as exc:  # pylint: disable=broad-except
        __log.exception('Exiftool invocation failed due to %s', exc)
        return None


def get_camera_sns(paths: Dict[str, Path]) -> Dict[str, str]:
    """Retrieve camera serial numbers

    Args:
        paths (Dict[str, Path]): Mapping of indices and paths

    Returns:
        Dict[str, str]: Mapping of corresponding camera serial numbers
    """
    __log = logging.getLogger('exiftool')
    try:
        with exiftool.ExifToolHelper(executable=settings.exiftool.path.as_posix()) as et:
            metadata = et.get_tags([path.as_posix() for path in paths.values()], [
                                   'MakerNotes:SerialNumber'])
        lookup = {Path(data['SourceFile']): data['MakerNotes:SerialNumber'].strip()
                  for data in metadata}
        return {cksum: lookup[path] for cksum, path in paths.items()}
    except Exception as exc:  # pylint: disable=broad-except
        __log.exception('Exiftool invocation failed due to %s', exc)
        return None


def get_project_export(project_id: int, label_studio_api_key: str, label_studio_host: str) -> Dict:
    """Retrieves the project export

    Args:
        project_id (int): Project to export

    Returns:
        Dict: LabelStudio project export
    """
    client = LabelStudio(
        base_url=f"https://{label_studio_host}",
        api_key=label_studio_api_key
    )
    response = client.projects.exports.create(
        project_id
    )
    if response.status != 'completed':
        raise RuntimeError('Snapshot failed')

    blob_iterator = client.projects.exports.download(
        project_id,
        export_pk=response.id
    )
    blob = BytesIO()
    for chunk in blob_iterator:
        blob.write(chunk)
    blob.seek(0)
    return json.load(blob)


def do_image_checksums(paths: List[Path]) -> Dict[Path, str]:
    """Computes multiple image checksums

    Args:
        paths (List[Path]): List of paths to compute

    Returns:
        Dict[Path, str]: Mapping of image paths and corresponding checksums
    """
    with multiprocessing.Pool() as pool:
        checksums = pool.map(get_file_checksum, paths)
    return {paths[idx]: checksums[idx] for idx in range(len(paths))}
