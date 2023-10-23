import requests
from bs4 import BeautifulSoup
import re
import csv
import psycopg2

BATCH_SIZE = 500
connection = psycopg2.connect(
    database="mb_world",
    user="username",
    password="password",
    host="localhost",
    port="5432"
)
cursor = connection.cursor()


def get_car_category_links():
    url = "https://mbworld.org/forums/"

    response = requests.get(url)
    links = []
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")

        tbox_divs = soup.find_all("div", class_="tbox")

        for tbox in tbox_divs:
            box_title = tbox.find("div", class_="trow tcat")
            if not box_title:
                continue
            box_title = box_title.find("div", class_="tcell").find("h2")
            if not box_title:
                continue
            box_name = box_title.find("a").text
            if "Mercedes-Benz" not in box_name:
                continue
            group_div = tbox.find("div", class_="trow-group")

            if group_div:
                link_divs = group_div.find_all("div", class_="trow text-center forumbit")
                for link_div in link_divs:
                    try:
                        link_cls = link_div.find("div", class_=re.compile("tcell text-left"))
                        link = link_cls.find("h3").find("a")
                    except Exception:
                        continue
                    if link:
                        href = link.get("href")
                        if "vendor" not in href:
                            links.append(href)

        print(len(links))

    else:
        print("Failed to retrieve the web page.")
    return links


def generate_urls_with_numbers(url, page_num):
    base_url = url.replace('.html', '')
    if url.endswith("/"):
        base_url = url[:-1]
    modified_urls = f"{base_url}-{page_num}.html"

    return modified_urls


def get_question_links(num_pages, base_url):
    question_list = []
    prev_url = None
    for page_num in range(1, num_pages + 1):
        mod_url = generate_urls_with_numbers(base_url, page_num)
        print(mod_url)
        url = mod_url.format(page_num)
        response = requests.get(url)
        if prev_url == response.url:
            print(f"Finished this URL. Total Questions: {len(question_list)}")
            break
        prev_url = response.url
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            tbox_divs = soup.find_all('div', class_='tbox')

            for tbox_div in tbox_divs:
                threadslist_div = tbox_div.find('div', id='threadslist')
                if not threadslist_div:
                    continue
                comment_start = threadslist_div.find(string=' google_ad_section_start ')
                if comment_start:
                    sibling = comment_start.find_next_sibling()
                    while sibling:
                        if sibling.name == 'div' and 'trow' in sibling.get('class', []):
                            link = sibling.find('div', class_='tcell alt1 text-left').find('div').find('h4').find('a')[
                                'href']
                            question_list.append(link)
                        sibling = sibling.find_next_sibling()

        else:
            print(f"Failed to retrieve the web page for page {page_num}. Status code:", response.status_code)
    print(question_list)
    return question_list


def get_all_question_links():
    start_index = 76
    cat_link_list = get_car_category_links()[start_index:]
    for index, link in enumerate(cat_link_list):
        print(f"Index: {index + start_index}")
        q_links = get_question_links(num_pages=1000, base_url=link)
        filename = 'urls.csv'
        with open(filename, mode='a') as file:
            file.write('\n'.join(q_links) + '\n')
        print(f"End Index {index + start_index}")


def create_qa_db(start_row):
    with open('urls.csv', mode='r', newline='', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        db_list = []
        last_row = start_row
        for _ in range(start_row - 1):
            next(csv_reader, None)
        for row in csv_reader:
            link = row[0]
            response = requests.get(link)
            print(f"Loading data for link: {link}")
            soup = BeautifulSoup(response.content, 'html.parser')
            div_elements = soup.select('div[id^="post_message_"]')
            if len(div_elements) <= 1:
                print(f"Finished EMPTY row: {last_row}")
                last_row += 1
                continue
            td_post = soup.find('div', id=lambda x: x and x.startswith('td_post_'))
            strong_element = td_post.find('strong')
            title = strong_element.get_text() if strong_element else 'No Title'
            match = re.search(r'/forums/([^/]+)/', link)
            category = None
            if match:
                category = match.group(1)
            qa_dict = {'title': title,
                       'question': '',
                       'category': category,
                       'responses': []}

            for index, div in enumerate(div_elements):
                text = div.get_text(strip=True)
                # Ensure 'text' does not end with an unescaped backslash
                if text.endswith('\\'):
                    text = text[:-1]
                text = remove_escapes(text)
                if index == 0:
                    qa_dict['question'] = text
                    continue
                qa_dict['responses'].append(text)
            db_list.append(qa_dict)
            print(f"Finished row: {last_row}")
            last_row += 1
            if len(db_list) % BATCH_SIZE == 0:
                save_db_batches(db_list)
                db_list = []
        save_db_batches(db_list)
    connection.close()


def save_db_batches(qa_dicts):
    for i in range(0, len(qa_dicts), BATCH_SIZE):
        batch = qa_dicts[i:i + BATCH_SIZE]

        for qa_dict in batch:
            cursor.execute("""
                INSERT INTO qa_data (title, question, category, responses)
                VALUES (%s, %s, %s, %s);
            """, (qa_dict['title'], qa_dict['question'], qa_dict['category'], qa_dict['responses']))
    connection.commit()
    print(f"------------Saved {BATCH_SIZE} rows")


def remove_escapes(text):
    # Use regular expression to remove escape sequences
    text = re.sub(r'\\[\\nrtbfuU"\']', '', text)
    # Use regular expression to remove surrogate pairs
    text = re.sub(r'[\ud800-\udbff][\udc00-\udfff]', '', text)
    return text


create_qa_db(start_row=179882)
