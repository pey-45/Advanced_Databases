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
import psycopg2.errorcodes
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
    """
    Creates table 'article'
    :param conn: open connection to the database
    :return: None
    """
    
    statement = """
                CREATE TABLE article (
                    code INT CONSTRAINT code_pkey PRIMARY KEY,
                    name VARCHAR(30) NOT NULL,
                    price NUMERIC(5, 2) CONSTRAINT price_non_negative check (price > 0)
                )
                """
        
    with conn.cursor() as cur:
        try:
            cur.execute(statement)
            conn.commit()
            print("Table 'article' created")
        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
                print("Error: table 'article' already exists")
            else:
                print(f"Unknown error: {e.pgcode}: {e.pgerror}")
            conn.rollback()


## ------------------------------------------------------------
def delete_table_article(conn):
    """
    Creates table 'article'
    :param conn: open connection to the database
    :return: None
    """
    
    statement = """
                DROP TABLE article
                """
        
    with conn.cursor() as cur:
        try:
            cur.execute(statement)
            conn.commit()
            print("Table 'article' deleted")
        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                print("Error: table 'article' does not exist")
            else:
                print(f"Undefined error: {e.pgcode}: {e.pgerror}")
            conn.rollback()
            


## ------------------------------------------------------------
def insert_article(conn):
    """
    Inserts a row on the table 'article' requesting the parameters by keyboard
    :param conn: open connection to the database
    :return: None
    """
    
    code = input("Code: ")
    code = None if code == "".strip() else int(code)
    name = input("Name: ")
    name = None if name == "".strip() else name
    price = input("Price: ")
    price = None if price == "".strip() else float(price)
        
    statement = """
                INSERT INTO article(code, name, price) 
                values(%s, %s, %s)
                """
    parameters = (code, name, price)
    
    with conn.cursor() as cur:
        try:
            cur.execute(statement, parameters)
            conn.commit()
            print("Article inserted")
        except psycopg2.Error as e:
            conn.rollback()
    

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
        
        if key == "q": 
            break
        
        elif key == "1": 
            create_table_article(conn)    
            
        elif key == "2":
            delete_table_article(conn)
            
        elif key == "3": 
            insert_article(conn)
        
        elif key == "4": 
            code = input("Code to delete: ")
    
            try:
                code = int(code)
            except ValueError:
                print("Invalid code")
                return

            delete_article(conn, code)
            
        elif key == "5": 
            text = input("Enter text to delete articles that contain it: ")
    
            if text.strip() == "":
                print("You must enter at least one character")
                return

            delete_article_with_text(conn, text)
            
        elif key == "6": 
            show_article_count(conn)
            
        elif key == "7": 
            code = input("Code from article to show: ")

            try:
                code = int(code)
            except ValueError:
                print("Invalid code")
                return

            show_article_details(conn, code)
            
        elif key == "8": 
            show_all_articles(conn)
            
        elif key == "9": 
            price = input("Enter the minimum price: ")
    
            try:
                price = float(price)
            except ValueError:
                print("Error: invalid price")
                return
        
            show_articles_with_minimum_price(conn, price)
            
        elif key == "10": 
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
            
        elif key == "11": 
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
def main():
    print("Connecting to PosgreSQL...")
    conn = connect_db()
    print("Connected")
    menu(conn)
    disconnect_db(conn)


## ------------------------------------------------------------
if __name__ == '__main__':
    main()
