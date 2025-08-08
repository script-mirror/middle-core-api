FROM python:3.13-alpine

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN apk add --no-cache \
    gcc \
    musl-dev \
    linux-headers \
    && pip install --no-cache-dir -r /app/requirements.txt \
    && apk del gcc musl-dev linux-headers

RUN apk add --no-cache \
    ffmpeg \
    tzdata \
    xvfb-run \
    && ln -sf /usr/share/zoneinfo/America/Sao_Paulo /etc/localtime \
    && echo "America/Sao_Paulo" > /etc/timezone

ENV LANG=pt_BR.UTF-8 \
    LC_ALL=pt_BR.UTF-8 \
    TZ=America/Sao_Paulo

COPY . /app

CMD ["fastapi", "run", "main.py", "--port", "8000"]
