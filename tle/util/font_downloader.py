import logging
import os
import urllib.request
from io import BytesIO
from zipfile import ZipFile

from tle import constants

# Direct download URLs for Noto CJK fonts - using smaller individual files
FONT_URLS = {
    'NotoSansCJK-Bold.ttc': 'https://raw.githubusercontent.com/googlefonts/noto-cjk/main/Sans/OTC/NotoSansCJK-Bold.ttc',
    'NotoSansCJK-Regular.ttc': 'https://raw.githubusercontent.com/googlefonts/noto-cjk/main/Sans/OTC/NotoSansCJK-Regular.ttc',
}
FONTS = [
    constants.NOTO_SANS_CJK_BOLD_FONT_PATH,
    constants.NOTO_SANS_CJK_REGULAR_FONT_PATH,
]

logger = logging.getLogger(__name__)


def _download(font_path):
    font = os.path.basename(font_path)
    url = FONT_URLS.get(font)
    if not url:
        logger.warning(f'No download URL configured for font: {font}, skipping.')
        return False
    
    logger.info(f'Downloading font `{font}` from {url}.')
    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        )
        os.makedirs(os.path.dirname(font_path), exist_ok=True)
        with urllib.request.urlopen(req, timeout=300) as resp:
            with open(font_path, 'wb') as f:
                f.write(resp.read())
        logger.info(f'Successfully downloaded font `{font}`.')
        return True
    except Exception as e:
        logger.warning(f'Failed to download font `{font}`: {e}. Graphs may not render CJK characters.')
        return False


def maybe_download():
    for font_path in FONTS:
        if not os.path.isfile(font_path):
            _download(font_path)
