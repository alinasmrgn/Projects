import os
import re
import psycopg2
import telebot
from telebot import types
import random
from random import randint
from pandas import DataFrame

BOT_TOKEN = os.environ.get('BOT_TOKEN')
bot = telebot.TeleBot(BOT_TOKEN)

class Database(object):

    conn = None

    def __init__(self):
        if Database.conn is None:
            try:
                Database.conn = psycopg2.connect(database = "polish",
                                user = "postgres",
                                host= 'localhost',
                                password = "postgres",
                                port = 5432)
            except Exception as error:
                print("Error: Connection not established {}".format(error))
            else:
                print("Connection established")

    def execute_query(self, sql):
        cur = Database.conn.cursor()
        if re.match(r'^select',sql):
            cur.execute(sql)
            result = cur.fetchall()
            cur.close()
            return result
        else:
            cur.execute(sql)
            cur.close()

database = Database()

select_all = database.execute_query("select * from pl_rus_dict ")
df = DataFrame(select_all, columns=['word_id', 'pl_word', 'rus_word'])

@bot.message_handler(commands=['start'])
def send_welcome(message):
    text =  "What day do you want to do?\nChoose one:\n/translate - translate your word to polish \n/quiz - game, where you need to choose the correct one \n/add - add your word to the DataBase"
    bot.reply_to(message, text, parse_mode="Markdown")


@bot.message_handler(commands=['translate'])
def input_word(message):
    text = "Please write a word to translate"
    sent_msg = bot.send_message(message.chat.id, text, parse_mode="Markdown")
    bot.register_next_step_handler(sent_msg, translate_or_add)

def translate_or_add(message):
    word = message.text
    global word_to_translate
    word_to_translate = word
    translation = get_translate(word)
    if translation:
        bot.send_message(message.chat.id, translation, parse_mode="Markdown")
    else:
        # No translation found, ask the user if they want to add it
        text = "No such word in the database. Do you want to add it to the database? (yes/no)"
        sent_msg = bot.send_message(message.chat.id, text, parse_mode="Markdown")
        bot.register_next_step_handler(sent_msg, handle_add_request)

def handle_add_request(message):
    response = message.text.lower()

    if response == "yes":
        # User wants to add the word
        text = "Please enter the translation for this word:"
        sent_msg = bot.send_message(message.chat.id, text, parse_mode="Markdown")
        bot.register_next_step_handler(sent_msg, add_translation)
    elif response == "no":
        bot.send_message(message.chat.id, "Okay, word not added.")
    else:
        bot.send_message(message.chat.id, "Invalid response. Please type 'yes' or 'no'.")

def add_translation(message):
    translation = message.text
    database.execute_query(f"INSERT INTO pl_rus_dict(pl_word,rus_word) VALUES('{translation}','{word_to_translate }')")
    Database.conn.commit()
    bot.send_message(message.chat.id, f"Word '{word_to_translate }' and translation '{translation}' added to the database.")


def get_translate(word):
    # Execute a command: create pl_rus_dict table
    if bool(re.search('[а-яА-Я]', word)):
        records = database.execute_query("select pl_word from pl_rus_dict where upper(rus_word) like upper('{words}')".format(words=word))
        translation_of_the_word = " ".join(str(row[0]) for row in records)
        return translation_of_the_word
    elif bool(re.search('[a-zA-Z]', word)):
        records = database.execute_query("select rus_word from pl_rus_dict where upper(pl_word) = upper('{words}')".format(words=word))
        translation_of_the_word = " ".join(str(row[0]) for row in records)
        return translation_of_the_word
    else:
        print("No such word in the DataBase\n Do you want to add it yo the db? yes/no")
    # Make the changes to the database persistent
    Database.conn.commit()

@bot.message_handler(commands=['quiz'])
def quiz(message):
    records = database.execute_query("select count(*) from pl_rus_dict ")
    count_of_the_word = " ".join(str(row[0]) for row in records)

    rand = randint(1, int(count_of_the_word) + 1)  # random word_id from 1 to count + 1
    rand1 = randint(1, int(count_of_the_word) + 1)
    rand2 = randint(1, int(count_of_the_word) + 1)
    rand3 = randint(1, int(count_of_the_word) + 1)

    rus = [i for i in df['rus_word'][df["word_id"] == rand]]
    rus_answer = rus[0]
    pl = [i for i in df['pl_word'][df["word_id"] == rand]]
    pl_answer = pl[0]

    fake1 = [i for i in df['pl_word'][df["word_id"] == rand1]]
    fake_answ1 = fake1[0]
    fake2 = [i for i in df['pl_word'][df["word_id"] == rand2]]
    fake_answ2 = fake2[0]
    fake3 = [i for i in df['pl_word'][df["word_id"] == rand3]]
    fake_answ3 = fake3[0]
    rand_order = random.randint(1, 4)

    if rand_order == 1:
        keyboard = [[types.InlineKeyboardButton("{}".format(pl_answer), callback_data=pl_answer)],
                    [types.InlineKeyboardButton("{}".format(fake_answ1), callback_data=fake_answ1)],
                    [types.InlineKeyboardButton("{}".format(fake_answ3), callback_data=fake_answ3)],
                    [types.InlineKeyboardButton("{}".format(fake_answ2), callback_data=fake_answ2)]]
    elif rand_order == 2:
        keyboard = [[types.InlineKeyboardButton("{}".format(fake_answ1), callback_data=fake_answ1)],
                    [types.InlineKeyboardButton("{}".format(pl_answer), callback_data=pl_answer)],
                    [types.InlineKeyboardButton("{}".format(fake_answ2), callback_data=fake_answ2)],
                    [types.InlineKeyboardButton("{}".format(fake_answ3), callback_data=fake_answ3)]]
    elif rand_order == 3:
        keyboard = [[types.InlineKeyboardButton("{}".format(fake_answ1), callback_data=fake_answ1)],
                    [types.InlineKeyboardButton("{}".format(fake_answ2), callback_data=fake_answ2)],
                    [types.InlineKeyboardButton("{}".format(pl_answer), callback_data=pl_answer)],
                    [types.InlineKeyboardButton("{}".format(fake_answ3), callback_data=fake_answ3)]]
    else:
        keyboard = [[types.InlineKeyboardButton("{}".format(fake_answ1), callback_data=fake_answ1)],
                    [types.InlineKeyboardButton("{}".format(fake_answ2), callback_data=fake_answ2)],
                    [types.InlineKeyboardButton("{}".format(fake_answ3), callback_data=fake_answ3)],
                    [types.InlineKeyboardButton("{}".format(pl_answer), callback_data=pl_answer)]]
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True) #InlineKeyboardMarkup
    for row in keyboard:
        markup.row(*row)

    text = "<b>Quiz time</b> 🤖\nThe right translation of the word {} is... \n".format(rus_answer)
    bot.send_message(message.chat.id, text, reply_markup=markup,parse_mode="HTML")

    @bot.message_handler(func=lambda message: message.text == pl_answer)
    def button1(message):
        bot.send_message(message.chat.id, "Correct answer")

    @bot.message_handler(func=lambda message: message.text == fake_answ1 or message.text == fake_answ2 or message.text == fake_answ3)
    def button2(message):
        bot.send_message(message.chat.id, f"Wrong! The correct answer was {pl_answer}")

bot.infinity_polling()




















































































































































