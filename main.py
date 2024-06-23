import psycopg2
123123
# Параметры подключения к базе данных
DATABASE_NAME = "cards"
DATABASE_USER = "admin_bd"
DATABASE_PASSWORD = "1q2w3e4r"
DATABASE_HOST = "localhost"
DATABASE_PORT = 5432

# Функция для подключения к базе данных
def connect_to_db():
    """Функция для подключения к базе данных."""
    try:
        conn = psycopg2.connect(
            database=DATABASE_NAME,
            user=DATABASE_USER,
            password=DATABASE_PASSWORD,
            host=DATABASE_HOST,
            port=DATABASE_PORT
        )
        print('Подключено к БД')
        return conn
    except Exception as e:  # Указываем конкретное исключение
        print(f'Подключение не установлено: {e}')
        return None  # Возвращаем None, если подключение не удалось

# Функция для получения данных о первых 50 клиентах
def get_owners_data(conn):
    """Функция для получения данных о первых 50 клиентах."""
    if conn is None:
        print("Ошибка: Отсутствует подключение к базе данных")
        return None

    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            o.owner_id,
            o.gender,
            o.education,
            o.contact_type,
            o.response_to_campaign,
            l.city,
            c.has_credit_card,
            c.bank,
            c.offer_type,
            cs.average_check,
            cs.current_balance,
            cs.credit,
            cs.number_of_cards
        FROM owners o
        LEFT JOIN owner_locations ol ON o.owner_id = ol.owner_id
        LEFT JOIN locations l ON ol.location_id = l.location_id
        LEFT JOIN cards c ON o.owner_id = c.owner_id
        LEFT JOIN card_statistics cs ON o.owner_id = cs.owner_id
        ORDER BY o.owner_id
        LIMIT 50;
    """)
    owners_data = cursor.fetchall()
    return owners_data

if __name__ == "__main__":
    conn = connect_to_db()
    if conn is not None:
        # Получение данных о клиентах
        owners_data = get_owners_data(conn)
        if owners_data is not None:
            # Вывод полученных данных
            for owner in owners_data:
                print(owner)


