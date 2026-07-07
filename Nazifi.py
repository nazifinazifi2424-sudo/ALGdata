import os
import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

# 1. SAKA BOT TOKEN DINKA ANAN
# Za ka iya saka shi kai tsaye ko ta Environment Variable a Render
BOT_TOKEN = os.getenv("BOT_TOKEN")

bot = telebot.TeleBot(BOT_TOKEN)


# Sanya ainihin Link din shafinka na Render Static Site anan

SHAFIN_HTML = "https://html-2zrw.onrender.com"



# HANDLER NA /start

@bot.message_handler(commands=['start'])

def welcome_start(message):

    uid = message.from_user.id



    kb = InlineKeyboardMarkup(row_width=2)



    # Mabuɗan da ke buɗe Mini App

    btn1 = InlineKeyboardButton("Data4", web_app=WebAppInfo(url=SHAFIN_HTML))

    btn2 = InlineKeyboardButton("Films", web_app=WebAppInfo(url=SHAFIN_HTML))

    btn3 = InlineKeyboardButton("Follow", web_app=WebAppInfo(url=SHAFIN_HTML))

    btn4 = InlineKeyboardButton("Nazifi", web_app=WebAppInfo(url=SHAFIN_HTML))



    kb.add(btn1, btn2)

    kb.add(btn3, btn4)



    bot.send_message(

        uid,

        "Sannu da zuwa! 👇",

        reply_markup=kb

    )



# Karɓar bayani idan an taɓa mabuɗi a Mini App

@bot.message_handler(content_types=['web_app_data'])

def handle_mini_app_data(message):

    uid = message.from_user.id

    shigo_da_data = message.web_app_data.data

    bot.send_message(uid, f"Ka danna mabuɗin: {shigo_da_data.upper()} gata!")

# HANDLER NA / start
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# HANDLER NA /start
@bot.message_handler(commands=['start'])
def welcome_start(message):
    uid = message.from_user.id

    kb = InlineKeyboardMarkup(row_width=2)

    btn1 = InlineKeyboardButton("Data4", callback_data="data4")
    btn2 = InlineKeyboardButton("Films", callback_data="films")
    btn3 = InlineKeyboardButton("Follow", callback_data="follow")
    btn4 = InlineKeyboardButton("Nazifi", callback_data="nazifi")

    kb.add(btn1, btn2)
    kb.add(btn3, btn4)

    bot.send_message(
        uid,
        "Sannu da zuwa! 👇",
        reply_markup=kb
    )



from flask import Flask
import threading
import os

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running"

def run_web():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    threading.Thread(target=run_web).start()
    print("Bot dinka ya tashi...")
    bot.infinity_polling(skip_pending=True)

