import os
import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

# 1. SAKA BOT TOKEN DINKA ANAN
# Za ka iya saka shi kai tsaye ko ta Environment Variable a Render
BOT_TOKEN = os.getenv("BOT_TOKEN")

bot = telebot.TeleBot(BOT_TOKEN)

# HANDLER NA /start
@bot.message_handler(commands=['start'])
def welcome_start(message):
    uid = message.from_user.id

    # Kirkiro ReplyKeyboardMarkup na ƙasa
    # resize_keyboard=True yana sa girmansu ya daidaitu da kowane irin screen na waya
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)

    # Kirkiro mabuɗan guda 4 da ka buƙata
    btn1 = KeyboardButton(text="Data4")
    btn2 = KeyboardButton(text="Films")
    btn3 = KeyboardButton(text="Follow")
    btn4 = KeyboardButton(text="Nazifi")

    # Jera mabuɗan biyu-biyu kamar yadda ka tsara tsarin
    kb.add(btn1, btn2)  # Layi na farko: Data4 da Films
    kb.add(btn3, btn4)  # Layi na biyu: Follow da Nazifi

    # Tura saƙon
    bot.send_message(
        uid, 
        "Sannu da zuwa! Gashi mabuɗan sun bayyana a ƙasa 👇", 
        reply_markup=kb
    )

# Wannan zai sa bot ɗin ya ci gaba da aiki ba tare da ya tsaya ba
if __name__ == "__main__":
    print("Bot ɗinka ya tashi yana aiki...")
    bot.infinity_polling()