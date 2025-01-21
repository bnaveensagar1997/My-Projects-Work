import mysql.connector
import requests
import pandas as pd
import time
import streamlit as st
import plotly.express as px
import json
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt

# Step 1: Data Extraction from Google Books API
def fetch_books_data(api_key, query, max_results=1000):
    books_data = []
    base_url = "https://www.googleapis.com/books/v1/volumes"
    start_index = 0

    while start_index < max_results:
        params = {
            "q": query,
            "startIndex": start_index,
            "maxResults": 40,
            "key": api_key
        }

        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            if "items" in data:
                books_data.extend(data["items"])
                start_index += 40
            else:
                break
        else:
            print(f"Error: {response.status_code}")
            break

    return books_data

# Step 2: Process and Structure the Data
def process_books_data(books_data, search_key):
    processed_data = []
    for book in books_data:
        volume_info = book.get("volumeInfo", {})
        sale_info = book.get("saleInfo", {})

        processed_data.append({
            "book_id": book.get("id"),
            "search_key": search_key,
            "book_title": volume_info.get("title"),
            "book_subtitle": volume_info.get("subtitle"),
            "book_authors": ", ".join(volume_info.get("authors", [])),
            "book_description": volume_info.get("description"),
            "industryIdentifiers": str(volume_info.get("industryIdentifiers", [])),
            "text_readingModes": volume_info.get("readingModes", {}).get("text"),
            "image_readingModes": volume_info.get("readingModes", {}).get("image"),
            "pageCount": volume_info.get("pageCount"),
            "categories": ", ".join(volume_info.get("categories", [])),
            "language": volume_info.get("language"),
            "imageLinks": volume_info.get("imageLinks", {}).get("thumbnail"),
            "ratingsCount": volume_info.get("ratingsCount"),
            "averageRating": volume_info.get("averageRating"),
            "country": sale_info.get("country"),
            "saleability": sale_info.get("saleability"),
            "isEbook": sale_info.get("isEbook"),
            "amount_listPrice": sale_info.get("listPrice", {}).get("amount"),
            "currencyCode_listPrice": sale_info.get("listPrice", {}).get("currencyCode"),
            "amount_retailPrice": sale_info.get("retailPrice", {}).get("amount"),
            "currencyCode_retailPrice": sale_info.get("retailPrice", {}).get("currencyCode"),
            "buyLink": sale_info.get("buyLink"),
            "year": volume_info.get("publishedDate"),
            "publisher": volume_info.get("publisher")
        })

    return processed_data

# Step 3: Store Data in SQL Database
def store_data_in_sql(db_config, processed_data):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS books (
        book_id VARCHAR(255) PRIMARY KEY,
        search_key VARCHAR(255),
        book_title VARCHAR(255),
        book_subtitle TEXT,
        book_authors TEXT,
        book_description TEXT,
        industryIdentifiers TEXT,
        text_readingModes BOOLEAN,
        image_readingModes BOOLEAN,
        pageCount INT,
        categories TEXT,
        language VARCHAR(10),
        imageLinks TEXT,
        ratingsCount INT,
        averageRating DECIMAL(3, 2),
        country VARCHAR(10),
        saleability VARCHAR(50),
        isEbook BOOLEAN,
        amount_listPrice DECIMAL(10, 2),
        currencyCode_listPrice VARCHAR(10),
        amount_retailPrice DECIMAL(10, 2),
        currencyCode_retailPrice VARCHAR(10),
        buyLink TEXT,
        year TEXT,
        publisher TEXT
    );
    """
    cursor.execute(create_table_query)

    insert_query = """
    INSERT INTO books (
        book_id, search_key, book_title, book_subtitle, book_authors, book_description,
        industryIdentifiers, text_readingModes, image_readingModes, pageCount,
        categories, language, imageLinks, ratingsCount, averageRating, country,
        saleability, isEbook, amount_listPrice, currencyCode_listPrice,
        amount_retailPrice, currencyCode_retailPrice, buyLink, year, publisher
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE book_title=VALUES(book_title);
    """

    for book in processed_data:
        cursor.execute(insert_query, (
            book["book_id"], book["search_key"], book["book_title"], book["book_subtitle"], book["book_authors"],
            book["book_description"], book["industryIdentifiers"], book["text_readingModes"], book["image_readingModes"], book["pageCount"],
            book["categories"], book["language"], book["imageLinks"], book["ratingsCount"], book["averageRating"],
            book["country"], book["saleability"], book["isEbook"], book["amount_listPrice"], book["currencyCode_listPrice"],
            book["amount_retailPrice"], book["currencyCode_retailPrice"], book["buyLink"], book["year"], book["publisher"]
        ))

    connection.commit()
    cursor.close()
    connection.close()

# Step 4: Create Streamlit Application
def create_streamlit_app():
    st.title("BookScape Explorer")

    # Database connection
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "root",
        "database": "books_data"
    }
    connection = mysql.connector.connect(**db_config)

    # Query options
    query_options = [
        "Check Availability of eBooks vs Physical Books",
        "Find the Publisher with the Most Books Published",
        "Identify the Publisher with the Highest Average Rating",
        "Get the Top 5 Most Expensive Books by Retail Price",
        "Find Books Published After 2010 with at Least 500 Pages",
        "List Books with Discounts Greater than 20%",
        "Find the Average Page Count for eBooks vs Physical Books",
        "Find the Top 3 Authors with the Most Books",
        "List Publishers with More than 10 Books",
        "Find the Average Page Count for Each Category",
        "Retrieve Books with More than 3 Authors",
        "Books with Ratings Count Greater Than the Average",
        "Books with the Same Author Published in the Same Year",
        "Books with a Specific Keyword in the Title",
        "Year with the Highest Average Book Price",
        "Count Authors Who Published 3 Consecutive Years",
        "Authors with Books Published in the Same Year under Different Publishers",
        "Average Retail Price of eBooks vs Physical Books",
        "Identify Books with Ratings as Outliers",
        "Publisher with the Highest Average Rating for More than 10 Books"
    ]

    query_choice = st.selectbox("Select a Query", query_options)

    query_dict = {
        "Check Availability of eBooks vs Physical Books": "SELECT isEbook, COUNT(*) as count FROM books GROUP BY isEbook;",
        "Find the Publisher with the Most Books Published": "SELECT publisher, COUNT(*) as count FROM books GROUP BY publisher ORDER BY count DESC LIMIT 1;",
        "Identify the Publisher with the Highest Average Rating": "SELECT publisher, AVG(averageRating) as avg_rating FROM books GROUP BY publisher ORDER BY avg_rating DESC LIMIT 1;",
        "Get the Top 5 Most Expensive Books by Retail Price": "SELECT book_title, amount_retailPrice FROM books ORDER BY amount_retailPrice DESC LIMIT 5;",
        "Find Books Published After 2010 with at Least 500 Pages": "SELECT book_title, pageCount, year FROM books WHERE pageCount >= 500 AND year > '2010';",
        "List Books with Discounts Greater than 20%": "SELECT book_title, amount_listPrice, amount_retailPrice FROM books WHERE (amount_listPrice - amount_retailPrice) / amount_listPrice > 0.2;",
        "Find the Average Page Count for eBooks vs Physical Books": "SELECT isEbook, AVG(pageCount) as avg_pageCount FROM books GROUP BY isEbook;",
        "Find the Top 3 Authors with the Most Books": "SELECT book_authors, COUNT(*) as book_count FROM books GROUP BY book_authors ORDER BY book_count DESC LIMIT 3;",
        "List Publishers with More than 10 Books": "SELECT publisher, COUNT(*) as count FROM books GROUP BY publisher HAVING count > 10;",
        "Find the Average Page Count for Each Category": "SELECT categories, AVG(pageCount) as avg_pageCount FROM books GROUP BY categories;",
        "Retrieve Books with More than 3 Authors": "SELECT book_title, book_authors FROM books WHERE LENGTH(book_authors) - LENGTH(REPLACE(book_authors, ',', '')) + 1 > 3;",
        "Books with Ratings Count Greater Than the Average": "SELECT book_title, ratingsCount FROM books WHERE ratingsCount > (SELECT AVG(ratingsCount) FROM books);",
        "Books with the Same Author Published in the Same Year": "SELECT book_authors, year, COUNT(*) FROM books GROUP BY book_authors, year HAVING COUNT(*) > 1;",
        "Books with a Specific Keyword in the Title": "SELECT book_title FROM books WHERE book_title LIKE '%keyword%';",
        "Year with the Highest Average Book Price": "SELECT year, AVG(amount_retailPrice) as avg_price FROM books GROUP BY year ORDER BY avg_price DESC LIMIT 1;",
        "Count Authors Who Published 3 Consecutive Years": "SELECT book_authors FROM books GROUP BY book_authors HAVING MAX(year) - MIN(year) >= 3;",
        "Authors with Books Published in the Same Year under Different Publishers": "SELECT book_authors, year, COUNT(DISTINCT publisher) as publisher_count FROM books GROUP BY book_authors, year HAVING publisher_count > 1;",
        "Average Retail Price of eBooks vs Physical Books": "SELECT isEbook, AVG(amount_retailPrice) as avg_price FROM books GROUP BY isEbook;",
        "Identify Books with Ratings as Outliers": "SELECT book_title, averageRating, ratingsCount FROM books WHERE averageRating > (SELECT AVG(averageRating) + 2 * STDDEV(averageRating) FROM books) OR averageRating < (SELECT AVG(averageRating) - 2 * STDDEV(averageRating) FROM books);",
        "Publisher with the Highest Average Rating for More than 10 Books": "SELECT publisher, AVG(averageRating) as avg_rating, COUNT(*) as book_count FROM books GROUP BY publisher HAVING book_count > 10 ORDER BY avg_rating DESC LIMIT 1;"
    }

    query = query_dict.get(query_choice)
    if query:
        df = pd.read_sql(query, connection)
        st.write(df)

        # Visualization
        if query_choice == "Check Availability of eBooks vs Physical Books":
            fig, ax = plt.subplots()
            df.plot(kind='bar', x='isEbook', y='count', legend=False, ax=ax)
            ax.set_title("Availability of eBooks vs Physical Books")
            ax.set_ylabel("Count")
            ax.set_xlabel("Type")
            st.pyplot(fig)

        if query_choice == "Find the Publisher with the Most Books Published":
            fig, ax = plt.subplots()
            df.plot(kind='bar', x='publisher', y='count', legend=False, ax=ax, color='orange')
            ax.set_title("Publisher with the Most Books Published")
            ax.set_ylabel("Count")
            st.pyplot(fig)

    connection.close()

# Run the Application
if __name__ == "__main__":
    api_key = "AIzaSyC6ko6Wpvv3DxqBBgesRnr9y7qRQR3pEsM"
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "root",
        "database": "books_data"
    }

    # Uncomment below lines to fetch, process, and store data
    # books_data = fetch_books_data(api_key, "fiction")
    # processed_data = process_books_data(books_data, "fiction")
    # store_data_in_sql(db_config, processed_data)

    # Streamlit app
    create_streamlit_app()
