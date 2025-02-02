#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# exerbda.py: 
# Programa python para completar seguindo o boletín de exercicios de BDA.
#
# Autor: Miguel Rodríguez Penabad (miguel.penabad@udc.es)
# Data de creación: 19-01-2021
#

import psycopg2
import psycopg2.extras
import sys

## ------------------------------------------------------------
def print_psycopg_error(e):
    print("Error")
    print(f"Exception: {type(e)}")
    print(f"Code: {e.pgcode}")
    print(f"Message: {e.pgerror}")
    

## ------------------------------------------------------------
def connect_db():
    try:
        conn = psycopg2.connect(
            host = "localhost",
            user = "pey",
            password = "1234",
            dbname = "db1"
        )
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        print_psycopg_error(e)
        sys.exit(1)


## ------------------------------------------------------------
def disconnect_db(conn):
    conn.commit()
    conn.close()


## ------------------------------------------------------------
def create_table_article(conn):
    statement = """
                CREATE TABLE article (
                    code INT PRIMARY KEY,
                    name VARCHAR(30) NOT NULL,
                    price NUMERIC(5, 2) CHECK (price >= 0)
                )
                """
    
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
        conn.commit()
        print("Table 'article' created")
    except psycopg2.Error as e:
        conn.rollback()
        print_psycopg_error(e)


## ------------------------------------------------------------
def delete_table_article(conn):
    statement = """
                DROP TABLE article
                """
    
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
        conn.commit()
        print("Table 'article' deleted")
    except psycopg2.Error as e:
        conn.rollback()
        print_psycopg_error(e)


## ------------------------------------------------------------
def insert_article(conn, code, name, price):
    statement = """
                INSERT INTO article(code, name, price) 
                values(%s, %s, %s)
                """
    parameters = (code, name, price)
    
    try:
        with conn.cursor() as cur:
            cur.execute(statement, parameters)
        conn.commit()
        print("Article inserted")
    except psycopg2.Error as e:
        conn.rollback()
        print_psycopg_error(e)
    

## ------------------------------------------------------------
def delete_article(conn, code):  
    statement = """
                DELETE FROM article
                WHERE code = %s 
                RETURNING code
                """
    parameters = (code,)
                
    try:
        with conn.cursor() as cur:
            cur.execute(statement, parameters)
            deleted = cur.fetchone()
        conn.commit()
        print("Article deleted" if deleted else "Article with specified code does not exist")
    except psycopg2.Error as e:
        conn.rollback()
        print_psycopg_error(e)   
        
        
## ------------------------------------------------------------
def delete_article_with_text(conn, text):
    statement = """
                WITH deleted AS (
                    DELETE FROM article
                    WHERE name ILIKE %s
                    RETURNING *
                )
                SELECT count(*) FROM deleted
                """
    parameters = ("%" + text + "%",)
    
    try:
        with conn.cursor() as cur:
            cur.execute(statement, parameters)
            deleted_count = cur.fetchone()[0]
        conn.commit()
        print(f"{deleted_count} article(s) were deleted")
    except psycopg2.Error as e:
        conn.rollback()
        print_psycopg_error(e)
            

## ------------------------------------------------------------
def show_article_count(conn):
    statement = """
                SELECT count(*)
                FROM article
                """
    
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
            count = cur.fetchone()[0]
        conn.commit()
        print(f"{count} articles found")
    except psycopg2.Error as e:
        conn.rollback()
        print_psycopg_error(e)
    
    
## ------------------------------------------------------------
def show_article_details(conn, code, control_tx = True):
    statement = """
                SELECT * 
                FROM article
                WHERE code = %s
                """
    parameters = (code,)
    
    try:
        with conn.cursor() as cur:
            cur.execute(statement, parameters)
            article = cur.fetchone()
        if control_tx:
            conn.commit()
        print(f"Code: {article[0]}, name: {article[1]}, price: {article[2]}")  
        return article[0]
    except psycopg2.Error as e:
        if control_tx:
            conn.rollback()
        print_psycopg_error(e) 
        return None
        

## ------------------------------------------------------------
def show_all_articles(conn):
    statement = """
                SELECT *
                FROM article
                ORDER BY code
                """

    try:
        with conn.cursor() as cur:
            cur.execute(statement)
            articles = []
            while True:
                row = cur.fetchone() 
                if row is None:
                    break
                articles.append(row)
        conn.commit()
        for article in articles:
            print(f"Code: {article[0]}, name: {article[1]}, price: {article[2]}")
    except psycopg2.Error as e:
        conn.rollback()
        print_psycopg_error(e)
        
        
## ------------------------------------------------------------
def show_articles_with_minimum_price(conn, price): 
    statement = """
                SELECT *
                FROM article
                WHERE price > %s
                ORDER BY code
                """
    parameters = (price,)
    
    try:
        with conn.cursor() as cur:
            cur.execute(statement, parameters)
            articles = cur.fetchall()
        conn.commit()
        for article in articles:
            print(f"Code: {article[0]}, name: {article[1]}, price: {article[2]}")
        print(f"Total: {len(articles)} articles")
    except psycopg2.Error as e:
        conn.rollback()
        print_psycopg_error(e)
        
    
## ------------------------------------------------------------
def edit_article(conn, code, name, price): 
    statement = """
                UPDATE article
                SET name = %s, price = %s
                WHERE code = %s
                """
    parameters = (name, price, code)
    
    try:
        with conn.cursor() as cur:
            cur.execute(statement, parameters)
        conn.commit()
        print("Modified article:")
        show_all_articles(conn)
    except psycopg2.Error as e:
        conn.rollback()
        print_psycopg_error(e)
    

## ------------------------------------------------------------
def increment_article_price(conn, code, increment):
    statement = """
                UPDATE article
                SET price = price + price * %s / 100
                WHERE code = %s;
                """
    parameters = (increment, code)
    
    try:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        
        with conn.cursor() as cur:
            cur.execute(statement, parameters)
            
        confirmation = input("End program execution? [y/...] ")
        if confirmation == "y" or confirmation == "Y":
            disconnect_db()
            exit(0)
        conn.commit()
        print("Modified article:")
        show_article_details(conn, code)
    except psycopg2.Error as e:
        conn.rollback()
        print_psycopg_error(e)
        

def option1(conn):
    create_table_article(conn)  
    
    
def option2(conn):
    delete_table_article(conn)
    
    
def option3(conn):
    print("Creating new article")
            
    try:
        code = int(input("Code: "))
    except ValueError: 
        print("Invalid code")
        return

    name = input("Name: ").strip()
    if not name:
        print("Invalid name")
        return
        
    price = input("Price (enter to omit): ")
    if price:
        try:
            price = float(price)
        except ValueError:
            print("Invalid price")
            return
    else:
        price = None
        
    insert_article(conn, code, name, price)
    
    
def option4(conn):
    code = input("Code to delete: ")
    
    try:
        code = int(code)
    except ValueError:
        print("Invalid code")
        return

    delete_article(conn, code)


def option5(conn):
    text = input("Enter text to delete articles that contain it: ")
    
    if text.strip() == "":
        print("You must enter at least one character")
        return

    delete_article_with_text(conn, text)


def option6(conn):
    show_article_count(conn)
    

def option7(conn):
    code = input("Code from article to show: ")

    try:
        code = int(code)
    except ValueError:
        print("Invalid code")
        return

    show_article_details(conn, code)
    

def option8(conn):
    show_all_articles(conn)


def option9(conn):
    price = input("Enter the minimum price: ")
    
    try:
        price = float(price)
    except ValueError:
        print("Error: invalid price")
        return
        
    show_articles_with_minimum_price(conn, price)


def option10(conn):
    code = input("Code from article to edit: ")

    try:
        code = int(code)
    except ValueError:
        print("Error: invalid code")
        return
        
    print("Article details:")
    show_article_details(conn, code, False)
    
    name = input("New name: ").strip()
    if not name:
        print("Enter a valid name")
        
    price = input("New price (enter to omit): ")
    if price:
        try:
            price = float(price)
        except ValueError:
            print("Enter a valid price or omit it")
            return
    else:
        price = None
        
    edit_article(conn, code, name, price)


def option11(conn):
    code = input("Code from article to increment price: ")

    try:
        code = int(code)
    except ValueError:
        print("Error: invalid code")
        return
    
    print("Article details:")
    show_article_details(conn, code, False)
    
    increment = input("Increment (%): ")
    try: 
        increment = float(increment)
    except ValueError:
        print("Enter a valid percentage")
        return
    
    increment_article_price(conn, code, increment)
    
    
        
        


## ------------------------------------------------------------
def menu(conn):
    MENU_TEXT = """
                --------- MENU ---------------------
                1  - Create table 'article'
                2  - Delete table 'article'
                3  - Insert article
                4  - Delete article
                5  - Delete articles by text
                6  - Show article count
                7  - Show article details
                8  - Show all articles
                9  - Show articles with minimum price
                10 - Edit article
                11 - Increment price
                q  - Exit
                ------------------------------------
                """
                
    while True:
        print(MENU_TEXT)
        key = input("Option: ")
        if key == "q": break
        elif key == "1": option1(conn)   
        elif key == "2": option2(conn)
        elif key == "3": option3(conn)
        elif key == "4": option4(conn)
        elif key == "5": option5(conn)
        elif key == "6": option6(conn)
        elif key == "7": option7(conn)
        elif key == "8": option8(conn)
        elif key == "9": option9(conn)
        elif key == "10": option10(conn)
        elif key == "11": option11(conn)
            
            
            
## ------------------------------------------------------------
def main():
    print("Connecting to PosgreSQL...")
    conn = connect_db()
    print("Connected")
    menu(conn)
    disconnect_db(conn)


## ------------------------------------------------------------
if __name__ == '__main__':
    main()
