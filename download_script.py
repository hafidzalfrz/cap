import requests
import os

file_numbers = range(1, 13)  # Generate numbers 1 to 12

# Iterate over the file numbers and generate URLs

urls = []
for number in file_numbers:
    # Format the number with leading zeros
    formatted_number = "{:02d}".format(number)
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{formatted_number}.parquet"
    urls.append(url)


download_directory = "download"
os.makedirs(download_directory, exist_ok=True)

# Iterate over the URLs and download the files
for url in urls:
    # Extract the filename from the URL
    filename = url.split("/")[-1]

    # Set the full path to the file
    filepath = os.path.join(download_directory, filename)

    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Save the file to disk
        with open(filepath, "wb") as file:
            file.write(response.content)
        print(f"File '{filename}' downloaded successfully.")
    else:
        print(f"Failed to download file from '{url}'. Status code: {response.status_code}")
