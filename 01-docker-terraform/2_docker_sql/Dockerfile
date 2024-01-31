# Building an image where data persists

# Base image to be used to build upon
FROM python:3.9.1

# Command to run upon build
RUN pip install pandas

# Setting landing point
ENTRYPOINT ["bash"]

# To build this image use this command [docker build -t <image:name> .]
# To run image use this command [docker run -it <image:name>]