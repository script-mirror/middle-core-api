from fastapi import APIRouter, HTTPException, File, UploadFile
from fastapi.responses import JSONResponse
from typing import Annotated
from typing import Optional
from . import service

router = APIRouter(prefix='/speech-to-text')


@router.post('',tags=['Speech to Text'])
def post_wav_file(
    file: Annotated[bytes, File()]
    ):
    return service.convert_audio_to_text(file)

