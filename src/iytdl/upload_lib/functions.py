__all__ = ["unquote_filename", "thumb_from_audio", "covert_to_jpg", "take_screen_shot"]

import re
import logging
import mutagen

from io import BytesIO
from pathlib import Path
from typing import Any, Optional, Tuple, Union, List
from math import ceil

from PIL import Image

from iytdl.upload_lib import ext
from iytdl.utils import run_command

logger = logging.getLogger(__name__)


def unquote_filename(filename: Union[Path, str]) -> Path:
    """
    Removes single and double quotes from filename to avoid ffmpeg errors
    due to unclosed quotation in filename

    Parameters:
    ----------
        - filename (`Union[Path, str]`): Full file name.

    Returns:
    -------
        `str`: New filename after renaming original file

    """
    file = Path(filename) if isinstance(filename, str) else filename
    un_quoted = file.parent.joinpath(re.sub(r"[\"']", "", file.name))
    if file.name != un_quoted.name:
        file.rename(un_quoted)
        return un_quoted.absolute()
    return file.absolute()


def thumb_from_audio(filename: Union[Path, str]) -> Optional[str]:
    """Extract album art from audio

    Parameters:
    ----------
        - filename (`Union[Path, str]`): audio file path.

    Returns:
    -------
        `Optional[str]`: if audio has album art

    """
    file = Path(filename) if isinstance(filename, str) else filename
    if not (audio_id3 := mutagen.File(str(file))):
        return
    thumb_path = file.parent.joinpath("album_art.jpg")
    for key in audio_id3.keys():
        if "APIC" in key and (album_art := getattr(audio_id3[key], "data", None)):
            thumb_path = file.parent.joinpath("album_art.jpg")
            with BytesIO(album_art) as img_io:
                with Image.open(img_io) as img:
                    img.convert("RGB").save(str(thumb_path), "JPEG")
            break
    if thumb_path.is_file():
        return str(thumb_path)


def covert_to_jpg(filename: Union[Path, str]) -> Tuple[str, Tuple[int]]:
    """Convert images to Telegram supported thumb

    Parameters:
    ----------
        - filename (`Union[Path, str]`): Image file path.

    Returns:
    -------
        `Tuple[str, Tuple[int]]`: (thumb_path, dimensions)

    """
    file = Path(filename) if isinstance(filename, str) else filename
    with Image.open(file) as img:
        if file.name.lower().endswith(ext.photo[:2]):
            thumb_path = str(file)
        else:
            thumb_path = str(file.parent.joinpath(f"{file.stem}.jpeg"))
            img.convert("RGB").save(thumb_path, "JPEG")
        size = img.size
    return thumb_path, size


async def get_duration(vid_path, **kwargs):
    try:
        cmd = [
            str(kwargs.get("ffprobe", "ffprobe")),
            "-i",
            f'"{str(vid_path)}"',
            "-v",
            "quiet",
            "-show_entries",
            "format=duration",
            "-hide_banner",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
        ]
        _dur, _rt_code = await run_command(" ".join(cmd), shell=True, silent=True)
        return 0 if _rt_code != 0 else int(float(_dur))
    except Exception:
        return 0


async def take_screen_shot(
    video_file: str, ttl: int = -1, **kwargs: Any
) -> Optional[str]:
    """Generate Thumbnail from video

    Parameters:
    ----------
        - video_file (`str`): Video file path.
        - ttl (`int`, optional): Timestamp (default `-1` i.e use ffprobe).
        - **kwargs (`Any`, optional) Pass ffmpeg and ffprobe custom path.

    Returns:
    -------
        `Optional[str]`: On Success
    """
    file = Path(video_file)
    ss_path = file.parent.joinpath(f"{file.stem}.jpg")
    vid_path = f'"{video_file}"'
    if ttl == -1:
        ttl = (await get_duration(vid_path, **kwargs)) // 2
    cmd = [
        str(kwargs.get("ffmpeg", "ffmpeg")),
        "-hide_banner",
        "-loglevel error",
        "-ss",
        str(ttl),
        "-i",
        vid_path,
        "-vframes",
        "1",
        f'"{str(ss_path)}"',
    ]
    rt_code = (await run_command(" ".join(cmd), shell=True, silent=True))[1]
    if rt_code == 0 and ss_path.is_file():
        return str(ss_path)


async def split_video(file_path, **kwargs: Any) -> List[Path]:
    start, cur_duration, result = 1, 0, []
    file = Path(file_path)
    split_size = 1.5 * 1024 * 1024 * 1024
    parts = ceil(file.stat().st_size / split_size)
    while start <= parts:
        new_file = file.parent.joinpath(
            "{name}.part{no}{ext}".format(
                name=file.stem, no=str(start).zfill(3), ext=file.suffix
            )
        )
        logger.info(f"Part No. {start} starts at {cur_duration}")
        cmd = [
            str(kwargs.get("ffmpeg", "ffmpeg")),
            "-i",
            f"'{file_path}'",
            "-ss",
            str(cur_duration),
            "-fs",
            str(split_size),
            "-map_chapters",
            "-1",
            "-c",
            "copy",
            f'"{str(new_file)}"',
        ]
        rt_code = (await run_command(" ".join(cmd), shell=True, silent=True))[1]
        if rt_code == 0 and new_file.is_file():
            result.append(unquote_filename(new_file.absolute()))
        new_duration = await get_duration(new_file)
        cur_duration += new_duration
        start += 1
        logger.info(f"Duration of {new_file} : {new_duration}")
    logger.info(f"File Splitted To : {len(result)}")
    return sorted(result)
