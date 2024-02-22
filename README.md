Задание

Цель: Написать приложение на Kafka Streams DSL или Processor API (по желанию)

Инструменты: Kafka Streams + Kafka Connect Datagen

Результат: Вы получите пример приложения с использованием Kafka Streams

Задание: Написать приложение, которое будет отправлять сообщение-алерт, если сумма денег заработанных по этому продукту 
(для каждой покупки сумма - это purchase.quantity * product.price) за последнюю минуту больше 3 000.
Для генерации данных использовать файлы purchase.avsc и product.avsc из репозитория 
https://github.com/netology-ds-team/kafka-streams-practice
