# Loading of data from telegram chats and analytics on that data
- *read_telega_dump.py* 
   : module to read data from Telegram chat dump. By dump I mean json file, resulted from "export history" for chat on desktop telegram. Reactions (emoji) are not included in that file.
- *read_telega_api.py* : ETL module to load data from telegram chat to parquet. Uses sync version of client Telethon library. Method *extract_messages(..)* can be used for incremental loading. It retrieves "reactions" from the messages as well.
- *telega2pandas.ipynb* Jupyter notebook for analytics, using pandas and natural language processing for one of the russian-speaking channels
- *\experiments* folder: Some experiments with async data extraction and with pyrogram library