import os
import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

# 1. SAKA BOT TOKEN DINKA ANAN
# Za ka iya saka shi kai tsaye ko ta Environment Variable a Render
BOT_TOKEN = os.getenv("BOT_TOKEN")

bot = telebot.TeleBot(BOT_TOKEN)

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

