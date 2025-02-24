import random
import string
from pydub import AudioSegment
import speech_recognition as sr


def convert_number_words(text):
    number_mapping = {
        'zero': '0', 'um': '1', 'dois': '2', 'três': '3', 'quatro': '4',
        'cinco': '5', 'seis': '6', 'sete': '7', 'oito': '8', 'nove': '9'
    }
    words = text.lower().split()
    result = []
    for word in words:
        result.append(number_mapping.get(word, word))
    return ''.join(result)

def convert_audio_to_text(file):
    audio = AudioSegment.from_wav(file)
    audio = audio.set_frame_rate(16000).set_channels(1)
    audio.export("/tmp/captcha_converted.wav", format="wav")
 
    try:
        recognizer = sr.Recognizer()
        with sr.AudioFile(f"tmp/{''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))}.wav") as source:
            audio_data = recognizer.record(source)
            captcha_text = recognizer.recognize_google(audio_data, language="pt-BR")
        print(captcha_text)
        captcha_text = convert_number_words(captcha_text)
        print(captcha_text)
        return {"text":captcha_text}
    except Exception as e:
        print("Erro ao converter áudio em texto:", e)