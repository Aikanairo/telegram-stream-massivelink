import sys
import os
import pymongo
import asyncio
import hashlib
from typing import Any, Optional, Union
from urllib.parse import quote_plus
from pyrogram import Client
import argparse

# Configurazione degli argomenti da linea di comando
parser = argparse.ArgumentParser(description="Genera link streaming.")
parser.add_argument("collezione", help="Nome della collezione MongoDB")
parser.add_argument("--add", nargs=2, metavar=("TITLE", "YEAR"), help="Filtra per Title e Year")
args = parser.parse_args()

nome_collezione = args.collezione
filter_title, filter_year = args.add if args.add else (None, None)

# Connessione al database MongoDB
client_mongo = pymongo.MongoClient("mongodb://")
db = client_mongo["telegram-app"]
collection = db[nome_collezione]

# Configura le tue credenziali API
api_id = ""
api_hash = ""
bot_token = ""
url = "http://ddns.net:8081/"

# Variabile per tenere traccia dello stato della connessione
client_connected = False

# Inizializza il client Pyrogram una sola volta
app = Client("my_bot", api_id=api_id, api_hash=api_hash, bot_token=bot_token)

# Funzione per inizializzare il client Pyrogram
async def initialize_client():
    global client_connected
    if not client_connected:
        await app.start()
        client_connected = True

async def get_file_unique_id(chat_id: int, message_id: int) -> str:
    # Verifica se il client è connesso prima di utilizzarlo
    if not client_connected:
        await initialize_client()

    message = await app.get_messages(chat_id, message_id)
    if not message:
        raise ValueError(f"Messaggio non trovato: chat_id={chat_id}, message_id={message_id}")
    
    media = get_media_from_message(message)
    if media:
        return media.file_unique_id
    else:
        return None

def get_media_from_message(message) -> Any:
    media_types = (
        "audio", "document", "photo", "sticker", 
        "animation", "video", "voice", "video_note",
    )
    for attr in media_types:
        media = getattr(message, attr, None)
        if media:
            return media

def get_hash(file_unique_id: str, length: int) -> str:
    long_hash = hashlib.sha256(file_unique_id.encode("UTF-8")).hexdigest()
    return long_hash[:length]

def genera_link_streaming(file_unique_id, file_name, message_id, chat_id):
    file_hash = get_hash(file_unique_id, 6)
    stream_link = f"{url}{message_id}/{quote_plus(file_name)}?hash={file_hash}&chid={chat_id}"
    return stream_link

async def close_client():
    global client_connected
    if client_connected:
        await app.stop()
        client_connected = False

def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
    print(f"Cartella creata: {path}")

def create_strm_file(filename, content):
    with open(filename, 'w') as file:
        file.write(content)
    print(f"strm creato in {filename}")

async def main():
    global client_connected
    if not client_connected:
        await initialize_client()

    query = {}
    if filter_title and filter_year:
        query = {"Title": filter_title, "Year": int(filter_year)}

    record_count = collection.count_documents(query)
    print(f"Totale record da elaborare: {record_count}")

    for record in collection.find(query):
        nome_canale = record["Nome Canale"]
        chat_id = int(record["Canale ID"])
        channel_dir = os.path.join("./", nome_canale)
        create_directory(channel_dir)

        for season in record.get("Seasons", []):
            title = record["Title"]
            year = record["Year"]
            title_year = f"{title} ({str(year)})" if year is not None else title
            print(f"Creando cartella per: {title_year}")

            title_year_dir = os.path.join(channel_dir, title_year)
            create_directory(title_year_dir)

            season_number = season.get("SeasonNumber")
            season_dir = os.path.join(title_year_dir, f"Season {season_number}") if season_number else title_year_dir
            create_directory(season_dir)

            for episode in season.get("Episodes", []):
                for content in episode.get("Contents", []):
                    message_id = content.get("Message ID")
                    if message_id:
                        file_unique_id = await get_file_unique_id(chat_id, message_id)
                        if file_unique_id:
                            file_name = content.get("Name")
                            if file_name:
                                link = genera_link_streaming(file_unique_id, file_name, message_id, chat_id)
                                filename_base = os.path.join(season_dir, file_name)
                                counter = 1
                                strm_filename = f"{filename_base}.strm"
                                while os.path.exists(strm_filename):
                                    counter += 1
                                    strm_filename = f"{filename_base} Part {counter}.strm"
                                create_strm_file(strm_filename, link)

    await close_client()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
