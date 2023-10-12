FROM python:3.11

WORKDIR /home/runner/work/streamlit/streamlit

COPY . .

RUN pip install -r requirements.txt

CMD [ "bash", "run.sh" ]