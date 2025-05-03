# Use the official Python image
FROM python:3.8

# Set the working directory to /app
WORKDIR /app

# Copy all files and directories from the current directory to the container
COPY . .

# Install project dependencies
RUN pip install -r requirements.txt

# Set the timezone in the container
ENV TZ=America/Argentina/Buenos_Aires

# CMD to execute the Python script (-u allows real-time output without buffering)
CMD ["python", "-u", "Script.py"]
