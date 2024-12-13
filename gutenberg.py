import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import json
import csv
import re
import random  # Importing random module for generating random ratings
import multiprocessing as mp


class CSVHandler:
    """Handles reading and writing to CSV files."""

    def __init__(self, file_name, fieldnames):
        """Initialize with file name and fieldnames."""
        self.file_name = file_name
        self.fieldnames = fieldnames

    def save_metadata_bulk(self, data):
        """Write a list of dictionaries to the CSV file."""
        with open(self.file_name, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=self.fieldnames)
            # Write header only if the file is empty
            if file.tell() == 0:
                writer.writeheader()
            writer.writerows(data)

    def load_metadata(self):
        """Read data from the CSV file into a list of dictionaries."""
        with open(self.file_name, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            return [row for row in reader]


class GutenbergScraper:
    """Scrapes metadata from Project Gutenberg."""

    BASE_URL = "https://www.gutenberg.org/ebooks/{}"
    HEADERS = {"User-Agent": "Mozilla/5.0"}

    @staticmethod
    def safe_find(soup, tag, **kwargs):
        """Safely find a tag in the soup and return its text."""
        element = soup.find(tag, **kwargs)
        return element.get_text(strip=True) if element else None

    @staticmethod
    def find_by_header(soup, header_text):
        """Find a value in a table based on the header text."""
        th_tag = soup.find("th", string=lambda text: text and header_text.lower() in text.lower())
        return th_tag.find_next_sibling("td").get_text(strip=True) if th_tag else None

    @staticmethod
    def extract_year(date_str):
        """Extract the year from a date string (handles "12 Dec 2024" format)."""
        try:
            return datetime.strptime(date_str, "%d %b %Y").year if date_str else None
        except ValueError:
            return None

    DEFAULT_IMAGE_URL = "https://www.gutenberg.org/gutenberg/pg-logo-129x80.png"

    def fetch_metadata(self, bookno):
        """Fetch metadata for a specific book."""
        url = self.BASE_URL.format(bookno)
        try:
            response = requests.get(url, headers=self.HEADERS, timeout=5)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching metadata for book {bookno}: {e}")
            return None

        soup = BeautifulSoup(response.content, "html.parser")
        
        image_tag = soup.find("img", {"class": "cover-art"})
        image_url = image_tag["src"] if image_tag else None
        
        if image_url and not image_url.startswith("http"):
            image_url = f"https://www.gutenberg.org{image_url}"
        
        if image_url == self.DEFAULT_IMAGE_URL:
            image_url = None

        ebook_no_tag = soup.find("th", string="EBook-No.")
        ebook_no = ebook_no_tag.find_next_sibling("td").get_text(strip=True) if ebook_no_tag else None

        year_of_publication_tag = soup.find("tr", property="dcterms:issued")
        year_of_publication = year_of_publication_tag.find("td", itemprop="datePublished") if year_of_publication_tag else None
        year_of_publication_text = year_of_publication.get_text() if year_of_publication else ""
        match = re.search(r'\d{4}', year_of_publication_text)
        if match:
            year_of_publication = match.group()
        else:
            print("No year found.")
        
        original_publication = self.safe_find(soup, "th", string="Original Publication")
        original_publication = self.safe_find(soup.find("th", string="Original Publication"), "td") if original_publication else None

        metadata = {
            "ISBN": ebook_no,
            "Book-Title": self.safe_find(soup, "td", itemprop="headline"),
            "Book-Author": self.safe_find(soup, "a", rel="marcrel:aut"),
            "Year-Of-Publication": year_of_publication,
            "Image-URL-S": image_url,
            "Image-URL-M": image_url,
            "Image-URL-L": image_url
        }
        return metadata

    def fetch_rating_data(self, user_id, books):
        """Fetch random ratings for a specific user for multiple books."""
        ratings = []
        for book in books:
            rating = random.randint(0, 100)
            ratings.append({
                "User-ID": user_id,
                "ISBN": book,
                "Book-Rating": rating
            })
        return ratings

    def generate_random_user_data(self, user_id):
        """Generate random location and age for a user."""
        locations = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']
        location = random.choice(locations)
        age = random.randint(18, 70)
        return {
            "User-ID": user_id,
            "Location": location,
            "Age": age
        }


def fetch_and_store_books(csv_file_metadata, csv_file_ratings, csv_file_users, book_range):
    """Fetch and store book metadata, ratings, and user data in separate CSV files."""
    metadata_handler = CSVHandler(csv_file_metadata, [
        "ISBN", "Book-Title", "Book-Author", "Year-Of-Publication", "Original-Publication", 
        "Publisher", "Image-URL-S", "Image-URL-M", "Image-URL-L"
    ])
    
    ratings_handler = CSVHandler(csv_file_ratings, [
        "User-ID", "ISBN", "Book-Rating"
    ])
    
    users_handler = CSVHandler(csv_file_users, [
        "User-ID", "Location", "Age"
    ])

    scraper = GutenbergScraper()
    batch_size = 50
    batch_metadata = []
    batch_ratings = []
    batch_users = []
    user_id_counter = 1  # Counter to generate user IDs

    for bookno in book_range:
        metadata = scraper.fetch_metadata(bookno)
        if metadata:
            batch_metadata.append(metadata)

        if random.random() < 0.05:  # 5% chance to generate a new user
            user_id = f"{user_id_counter}"
            user_data = scraper.generate_random_user_data(user_id)
            user_id_counter += 1
            if user_data:
                batch_users.append(user_data)

            # Simulate the user rating this book and potentially others
            ratings = scraper.fetch_rating_data(user_id, [bookno])
            batch_ratings.extend(ratings)

        if len(batch_metadata) >= batch_size:
            metadata_handler.save_metadata_bulk(batch_metadata)
            ratings_handler.save_metadata_bulk(batch_ratings)
            users_handler.save_metadata_bulk(batch_users)
            batch_metadata.clear()
            batch_ratings.clear()
            batch_users.clear()

    if batch_metadata:
        metadata_handler.save_metadata_bulk(batch_metadata)
    if batch_ratings:
        ratings_handler.save_metadata_bulk(batch_ratings)
    if batch_users:
        users_handler.save_metadata_bulk(batch_users)


def format_time(seconds):
    """Format time in seconds into a human-readable string."""
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours)}h {int(minutes)}m {seconds:.2f}s"


def main():
    """Main entry point for the script."""
    start_time = time.time()

    csv_file_metadata = "BX-Books.csv"
    csv_file_ratings = "BX-Book-Ratings.csv"
    csv_file_users = "BX-Users.csv"

    start, end = 1, 1000
    num_processes = 4
    book_ranges = [
        range(i, min(i + (end - start + 1) // num_processes, end + 1))
        for i in range(start, end + 1, (end - start + 1) // num_processes)
    ]

    processes = [
        mp.Process(target=fetch_and_store_books, args=(csv_file_metadata, csv_file_ratings, csv_file_users, book_range))
        for book_range in book_ranges
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    end_time = time.time()
    total_time = end_time - start_time
    formatted_time = format_time(total_time)
    print(f"Total execution time: {formatted_time}")


if __name__ == "__main__":
    main()
