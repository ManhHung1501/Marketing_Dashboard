import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import *
import secrets
from django.conf import settings
from django.template.loader import render_to_string
import os
import django
import prettytable as pt
from tabulate import tabulate


API_KEY = ""
os.environ.setdefault(
    'DJANGO_SETTINGS_MODULE',
    'WebStatistics.settings')
django.setup()

#def start(update: Update, context: CallbackContext) -> None:
#    chat_id = update.effective_chat.id
#    print(chat_id)
#    context.bot.send_message(chat_id=chat_id,
#            text=f"Thank you for using our telegram bot! We will send you notifications here!")
#def handle_message(update, context):
#    text = str(update.message.text).lower()
#    print(update)
#    update.message.reply_text(f"Hi, {update['message']['chat']['first_name']}")

def post_event_on_telegram(event):
    message_html = render_to_string('app/telegram_post.html', {
        'event': event
    })
    
    telegram_settings = settings.TELEGRAM
    print(telegram_settings)
    bot = telegram.Bot(token=API_KEY)
    bot.send_message(chat_id="@%s" % telegram_settings['channel_name'],
                     text=message_html, parse_mode=telegram.ParseMode.HTML)

#if __name__ == '__main__':
#    telegram_settings = settings.TELEGRAM
#    updater = Updater(API_KEY, use_context = True)
#    dp = updater.dispatcher
#    dp.add_handler(CommandHandler('start', start))
#    dp.add_handler(MessageHandler(Filters.text, handle_message))
#    # Start the Bot
#    updater.start_polling( )
#    # timeout=300

#    # Run the bot until the user presses Ctrl-C or the process receives SIGINT,
#    # SIGTERM or SIGABRT
#    updater.idle()
async def send_table(headers, data, msg, channel):
    # tb = pt.PrettyTable()
    # tb.field_names = headers
    # if len(data) <= 0:
    #     return 0

    
    # tb.add_rows(data)
    
    tb = tabulate(data, headers=headers, tablefmt='grid')

    telegram_settings = settings.TELEGRAM
    # print(f'<pre>{tb}</pre>')
    bot = telegram.Bot(token=API_KEY)
    message_html = f'<b>{msg}</b>\n<pre>{tb}</pre>'
    await bot.send_message(chat_id=channel,
                     text=message_html, parse_mode='HTML')
    return 1

async def send_msg(msg, channel):
    telegram_settings = settings.TELEGRAM
    bot = telegram.Bot(token=API_KEY)
    await bot.send_message(chat_id=channel, text=msg)
    return 1

