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

    # 1. Kirkiro InlineKeyboardMarkup (Mabuɗan maƙale ƙarƙashin saƙo)
    kb = InlineKeyboardMarkup(row_width=2)

    # 2. Kirkiro mabuɗan taɓawa guda 4 daidai yadda kake so
    btn1 = InlineKeyboardButton(text="Data4", callback_data="data4")
    btn2 = InlineKeyboardButton(text="Films", callback_data="films")
    btn3 = InlineKeyboardButton(text="Follow", callback_data="follow")
    btn4 = InlineKeyboardButton(text="Nazifi", callback_data="nazifi")

    # 3. Jera mabuɗan biyu-biyu a kowane layi ba tare da ɓata tsari ba
    kb.add(btn1, btn2)  # Layi na farko
    kb.add(btn3, btn4)  # Layi na biyu

    # Tura saƙon tare da mabuɗan Inline
    bot.send_message(
        uid, 
        "Sannu da zuwa! Gashi mabuɗan sun bayyana a ƙasa 👇", 
        reply_markup=kb
    )

# Wannan zai sa bot ɗin ya ci gaba da aiki ba tare da ya tsaya ba
if __name__ == "__main__":
    print("Bot ɗinka ya tashi yana aiki...")
    bot.infinity_polling()