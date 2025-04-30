""" All constants of app will be declare here... """

# Python Packages
from decouple import config


KAFKA_BROKER        =   config('KAFKA_BROKER')
KAFKA_TOPIC_NAME    =   config('KAFKA_TOPIC_NAME')
