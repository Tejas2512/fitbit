# init a base image
FROM python:3.6
# define the present working directory
WORKDIR /usr/app/
# copy all the content into working directory
ADD . /usr/app/
# install all dependency for this project
RUN pip install -r requirements.txt
# define the command to start the container
CMD python main.py

# command we use to build docker image:

# docker image build -t <REPOSITORY> .
# docker images
# docker ps
# docker run -p 5000:5000 -d <REPOSITORY>
# docker login dockerfitbit.azurecr.io
# docker push dockerfitbit.azurecr.io/mlfitbit:latest
# docker stop <containerID>
# docker system prune
