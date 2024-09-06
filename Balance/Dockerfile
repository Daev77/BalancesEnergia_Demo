FROM python:3.10

WORKDIR /Balance

COPY requirements.txt /Balance/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /Balance/requirements.txt

COPY . /Balance

CMD ["python", "balance.py"]
