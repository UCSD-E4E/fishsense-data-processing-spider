'''Discovery backend
'''
import datetime as dt
import json
import multiprocessing
import multiprocessing.pool
from hashlib import md5
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
from label_studio_sdk.client import LabelStudio
from label_studio_sdk.projects.client_ext import ProjectExt
from label_studio_sdk.types import BaseUser
from PIL import ExifTags, Image


def get_image_date(path: Path) -> Optional[dt.datetime]:
    try:
        img = Image.open(path)
        exif = img.getexif()
        creation_time_str = exif.get(ExifTags.Base.DateTime)
        creation_time = dt.datetime.strptime(
            creation_time_str, '%Y:%m:%d %H:%M:%S')
        return creation_time
    except Exception:
        return None


def get_dive_date(path: Path) -> Tuple[dt.date, bool, bool]:
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
    cksum = md5()
    with open(path, 'rb') as handle:
        for blob in iter(lambda: handle.read(8192), b''):
            cksum.update(blob)
    return cksum.hexdigest()


def get_dive_checksum(path: Path) -> str:
    reference_data = sorted(path.glob('*.ORF'))
    with multiprocessing.Pool() as pool:
        checksums = pool.map(get_file_checksum, reference_data)
    cksum = md5()
    for idx, file in enumerate(reference_data):
        cksum.update(f'{file.name}:{checksums[idx]}\n'.encode())
    return cksum.hexdigest()


def get_camera_sn(path: Path) -> Optional[str]:
    try:
        img = Image.open(path)
        exif = img.getexif()
        return str(exif.get(ExifTags.Base.CameraSerialNumber))
    except Exception:
        return None


def get_camera_sns(paths: Iterable[Path]) -> List[str]:
    with multiprocessing.Pool() as pool:
        sns = pool.map(get_camera_sn, paths)
    return sns


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
    with multiprocessing.Pool() as pool:
        checksums = pool.map(get_file_checksum, paths)
    return {paths[idx]: checksums[idx] for idx in range(len(paths))}
