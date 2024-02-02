# Building an image where data persists

# Base image to be used to build upon
FROM python:3.9.1

# Command to run upon build
RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

# Copy file from current working directory docker directory
# First create a /app directory then cd into it 
WORKDIR /app
COPY injest_data1.py yellow_tripdata_2021-01.csv /app/ 

# Setting landing point
# Loads a bash interactive terminal upon running the image
ENTRYPOINT ["bash"]

# Runs the pyton scripts upon running the image
# ENTRYPOINT [ "python", "ingest_data1.py" ]

# To build this image use this command [docker build -t my_image:01 .]
# To run image use this command [docker run -it my_image:01]