import mysql.connector
import urllib
from bs4 import BeautifulSoup
import urllib2
import json
from threading import Thread
import Queue
from slimit import ast
from slimit.parser import Parser
from slimit.visitors import nodevisitor
import sys
import time

reload(sys)
sys.setdefaultencoding('utf8')


header = {'User-Agent': "Mozilla/5.0 (Windows NT 6.1; WOW64)" +
          " AppleWebKit/537.36 (KHTML, like Gecko)" +
          " Chrome/43.0.2357.134 Safari/537.36"
          }


def similar(a, b):
    a = a.lower()
    b = b.lower()
    words = a.split(' ')
    totalNum = len(words)
    count = 0
    for w in words:
        if w.startswith("("):
            totalNum -= 1
        else:
            if w in b:
                count += 1
    return float(count * 1.0 / totalNum * 1.0) * 100


def appendToList(listFac, listFacHotels, item, HotelCode, extra):
    check = True
    if len(listFac) == 0:
        check = True
    else:
        for f in listFac:
            if(item[1].replace(
                ' ', '').lower() == f[1].replace(' ', '').lower() and
                item[2].replace(
                    ' ', '').lower() == f[2].replace(' ', '').lower()):
                check = False
    if check:
        listFac.append(item)
    listFacHotels.append(tuple((item[1], item[2], HotelCode, extra)))


def get_soup(url, header):
    return BeautifulSoup(urllib2.urlopen(
        urllib2.Request(url, headers=header)), 'lxml')


def extractDescriptionAndFacilities(facilitiesList, hotelFacilitiesList, url,
                                    hotel_code, countries_name, hotel_name):
    ##############################
    # Fetching Response from URL #
    ##############################
    soup = get_soup(url, header)
    ##############################################################
    # Extracting Country, City and Hotel Name of Booking Website #
    ##############################################################
    y = soup.find('script', type="text/javascript").text
    parser = Parser()
    tree = parser.parse(y)
    fields = {getattr(node.left, 'value', ''): getattr(node.right, 'value', '')
              for node in nodevisitor.visit(tree)
              if isinstance(node, ast.Assign)}
    if not fields['dest_cc'].replace('\'', '') == countries_name.lower():
        que.put("('" + hotel_code + "', " +
                "'N/A'" + ", " + "'N/A'" + ", " + "'N/A'" + ", 'N/A', " +
                "'N/A'" + ", " + "'N/A'" + ", " + "'N/A'" + ", 'N/A', " +
                "'N/A'" + ", " + "'N/A'" +
                ")")
    else:
        bookingcountryName = fields['country_name'].replace('\'', '')
        bookingcityName = fields['city_name'].replace('\'', '')
        bookinghotelName = fields['hotel_name'].replace('\'', '')
        bookingSimilarity = similar(hotel_name, bookinghotelName)
        ##########################
        # Extracting Description #
        ##########################
        s = soup.find("div", {"class": "hp_desc_main_content"})
        desc_main = s.find_all("p")
        if desc_main is not None:
            description = ''
            for a in desc_main:
                if "Booking.com" in a.get_text().encode(
                        'utf8'):
                    break
                else:
                    description += a.get_text().encode(
                        'utf8').strip().replace(
                        '\n', '').replace('\'', '').replace('\t', '')
            description = description.replace(',', ' ').replace('\'', '')
            description = description.encode('utf8')
        #########################
        # Extracting Facilities #
        #########################
        x = soup.find_all(
            "div", {"class": "facilitiesChecklistSection"})
        extra = ''
        if x is not None:
            for xy in x:
                category = xy.find("h5").get_text().encode(
                    'utf8').strip().replace('\n', '')
                for xyz in xy.find_all("li"):
                    try:
                        name = xyz.get_text().encode(
                            'utf8').strip().replace('\n', '').split('(')[0]
                        extra = ''
                        if len(xyz.get_text().encode(
                                'utf8').strip().replace(
                                '\n', '').split('(')) < 2:
                            xyz.get_text().encode(
                                'utf8').strip().replace('\n', '').split('(')[1]
                        for ext in xrange(1, len(xyz.get_text().encode(
                                'utf8').strip().replace('\n', '').split('('))):
                            extra += '('
                            extra += xyz.get_text().encode(
                                'utf8').strip().replace(
                                '\n', '').split('(')[ext]
                        appendToList(facilitiesList,
                                     hotelFacilitiesList,
                                     tuple(('', category, name)),
                                     hotel_code,
                                     extra)
                    except Exception:
                        extra = ''
                        name = xyz.get_text().encode(
                            'utf8').strip().replace('\n', '')
                        appendToList(facilitiesList,
                                     hotelFacilitiesList,
                                     tuple(('', category, name)),
                                     hotel_code,
                                     extra)
        ###########################################
        # Extract Latitude and Longitude of Hotel #
        ###########################################
        bookingScripts = soup.find_all('script')
        for loc in bookingScripts:
            if 'booking.env.b_map_center_latitude' in loc.text:
                locationScript = loc.text
        bookingLat = locationScript.split(
            'booking.env.b_map_center_latitude = ')[1].split(';')[0]
        bookingLong = locationScript.split(
            'booking.env.b_map_center_longitude = ')[1].split(';')[0]
        #######################
        # Extract Star Rating #
        #######################
        starRate = ''
        hotelRates = soup.find('span', {"class": "hp__hotel_ratings__stars"})
        if hotelRates is not None:
            if hotelRates.find(
                    'svg', {"class": '-sprite-ratings_stars_5'}) is not None:
                starRate = '5 Stars'
            elif hotelRates.find(
                    'svg', {"class": '-sprite-ratings_stars_4'}) is not None:
                starRate = '4 Stars'
            elif hotelRates.find(
                    'svg', {"class": '-sprite-ratings_stars_3'}) is not None:
                starRate = '3 Stars'
            elif hotelRates.find(
                    'svg', {"class": '-sprite-ratings_stars_2'}) is not None:
                starRate = '2 Stars'
            elif hotelRates.find(
                    'svg', {"class": '-sprite-ratings_stars_1'}) is not None:
                starRate = '1 Star'
        ######################################
        # Extract Array of Images in Booking #
        ######################################
        image_links = ''
        try:
            viz = soup.find('div', {"id": "photos_distinct"})
            v = viz.find_all('a')
            if v is not None:
                for img in v:
                    if img['href'] == '#':
                        try:
                            imgToPrint = img['style'].split('(')[1]
                            imgToPrint = imgToPrint.split(')')[0]
                            image_links += imgToPrint + '(x)'
                        except Exception:
                            pass
                    else:
                        image_links += img['href'] + '(x)'
                image_links = image_links[:-3]
        except Exception:
            scy = soup.find_all('script')
            for sc in scy:
                if 'hotelPhotos' in sc.text:
                    scyOrigin = sc.text
            temp = scyOrigin.split('hotelPhotos:')
            temp2 = temp[1].split('],')
            temp3 = temp2[0]
            while not temp3.find('large_url') == -1:
                temp3 = temp3[temp3.find('large_url') + 12:]
                temp4 = temp3[:temp3.find('\'')]
                image_links += temp4 + '(x)'
            image_links = image_links[:-3]
        ####################################
        # Returning Queries for SQL UPDATE #
        ####################################
        if (s is not None and
            x is not None and
                y is not None):
            return ("('" + hotel_code + "', '" +
                    bookingcountryName + "', '" +
                    bookingcityName + "', '" +
                    url + "', '" +
                    str(bookingSimilarity) + "', '" +
                    bookingLat + "', '" +
                    bookingLong + "', '" +
                    starRate + "', '" +
                    bookinghotelName + "', '" +
                    description + "', '" + image_links + "')")


def extractImageAndLink(facilitiesList, hotelFacilitiesList, hotel_code,
                        hotel_name, destination_name, countries_name, que):
    q = urllib.urlencode({"q": hotel_name.encode('utf-8').strip() +
                          ' ' + destination_name.encode('utf-8').strip()})
    c = countries_name
    url = ("https://www.google.com.eg/search?" + q + "+site:booking.com" +
           "&lr=&cr=country" + c + "&hl=en&tbs=ctr:country" +
           c + "&source=lnms&tbm=isch&sa=X&ved=0ahUKEwj93p3QkJPVAhUC5x" +
           "oKHRftDusQ_AUICigB&biw=1301&bih=678")
    try:
        soup = get_soup(url, header)
        s = soup.find("div", {"class": "rg_meta"})
        if s is not None:
            website = json.loads(s.text)["ru"]
            website_url = website + '?lang=en;'
            if 'searchresults' in website_url:
                que.put("('" + hotel_code + "', " +
                        "'N/A'" + ", " + "'N/A'" +
                        ", " + "'N/A'" + ", 'N/A', " +
                        "'N/A'" + ", " + "'N/A'" + ", " +
                        "'N/A'" + ", 'N/A', " +
                        "'N/A'" + ", " + "'N/A'" +
                        ")")
            else:
                que.put(extractDescriptionAndFacilities(facilitiesList,
                                                        hotelFacilitiesList,
                                                        website_url,
                                                        hotel_code,
                                                        countries_name,
                                                        hotel_name))
        else:
            que.put("('" + hotel_code + "', " +
                    "'N/A'" + ", " + "'N/A'" + ", " + "'N/A'" + ", 'N/A', " +
                    "'N/A'" + ", " + "'N/A'" + ", " + "'N/A'" + ", 'N/A', " +
                    "'N/A'" + ", " + "'N/A'" +
                    ")")
    except Exception as e:
        print "Exception bypassed!" + str(e)
        pass


while True:
    test_db = 0
    update_db = 1
    print_trace = 1
    if test_db:
        cnx = mysql.connector.connect(user='root', password='',
                                      host='127.0.0.1',
                                      database='TresJolie')
    else:
        cnx = mysql.connector.connect(user='root',
                                      password='PASSWORD',
                                      host='13.65.193.223',
                                      database='tres_jolie_db')
    cursor = cnx.cursor()
    queryFacilities = ("SELECT * FROM facilities AS f")
    cursor.execute(queryFacilities)
    facilitiesList = []
    hotelFacilitiesList = []
    # countF = 0
    for (identification, facility_category, facility_name) in cursor:
        facilitiesList.append(tuple((
            identification, facility_category, facility_name)))
        # countF += 1
    # print countF
    query = ("SELECT h.HotelCode, h.Name, d.Name, con.ISO FROM hotels as h " +
             "INNER JOIN destinations as d ON d.DestinationId=h.destinationID " +
             "INNER JOIN countries as con ON d.countryID = con.CountryID" +
             " WHERE h.booking_imageLinks is null ORDER BY RAND() LIMIT 50")
    cursor.execute(query)
    que = Queue.Queue()
    values = ''
    threadlist = []
    for (hotel_code, hotel_name, destination_name, countries_name) in cursor:
        t = Thread(target=extractImageAndLink, args=(facilitiesList,
                                                     hotelFacilitiesList,
                                                     hotel_code,
                                                     hotel_name,
                                                     destination_name,
                                                     countries_name, que))
        t.start()
        threadlist.append(t)
        time.sleep(2)
    cursor.close()
    cnx.close()
    for t in threadlist:
        t.join()
    valuesFacilities = ''
    for f in facilitiesList:
        valuesFacilities += ("('" + str(f[0]) + "', '" +
                             f[1].replace(
                             '\'', '') + "', '" + f[2].replace(
                             '\'', '') + "'), ")
    valuesFacilities = valuesFacilities[:-2]
    if not que.empty():
        while not que.empty():
            valueOfQueue = que.get()
            if valueOfQueue is None:
                update_db = 0
            else:
                values += valueOfQueue + ", "
        values = values[:-2]
        if update_db:
            if test_db:
                cnx = mysql.connector.connect(user='root', password='',
                                              host='127.0.0.1',
                                              database='TresJolie')
            else:
                cnx = mysql.connector.connect(user='root',
                                              password='PASSWORD',
                                              host='13.65.193.223',
                                              database='tres_jolie_db')
            cursor = cnx.cursor()
            queryUpdate = ("INSERT INTO hotels " +
                           "(HotelCode, booking_countryName, booking_cityName, " +
                           "booking_website, booking_similarityPercentage, " +
                           "booking_latitude, booking_longitude, " +
                           "booking_starAverage, booking_hotelName, " +
                           "booking_description, booking_imageLinks) " +
                           "VALUES " + values + " " +
                           "ON DUPLICATE KEY UPDATE " +
                           "booking_countryName=VALUES(booking_countryName), " +
                           "booking_cityName=VALUES(booking_cityName), " +
                           "booking_website=VALUES(booking_website), " +
                           "booking_similarityPercentage=VALUES(" +
                           "booking_similarityPercentage), " +
                           "booking_latitude=VALUES(booking_latitude), " +
                           "booking_longitude=VALUES(booking_longitude), " +
                           "booking_starAverage=VALUES(booking_starAverage), " +
                           "booking_hotelName=VALUES(booking_hotelName), " +
                           "booking_description=VALUES(booking_description), " +
                           "booking_imageLinks=VALUES(booking_imageLinks)" +
                           "")
            cursor.execute(queryUpdate)
            cnx.commit()
            queryUpdateFacilities = ("INSERT INTO facilities " +
                                     "(id, facility_category, facility_name) " +
                                     "VALUES " + valuesFacilities + " " +
                                     "ON DUPLICATE KEY UPDATE " +
                                     "facility_category=VALUES(" +
                                     "facility_category), " +
                                     "facility_name=VALUES(" +
                                     "facility_name)")
            cursor.execute(queryUpdateFacilities)
            cnx.commit()
            #################################
            # Fetching Committed Facilities #
            #################################
            queryFacilitiesUpdated = ("SELECT * FROM facilities AS f")
            cursor.execute(queryFacilitiesUpdated)
            facilitiesListUpdated = []
            for (identification, facility_category, facility_name) in cursor:
                facilitiesListUpdated.append(tuple((
                    identification, facility_category, facility_name)))
            cursor.close()
            cnx.close()
            #################################
            # Add Specific Hotel Facilities #
            #################################
            valuesHotelFacilities = ''
            for h in hotelFacilitiesList:
                for s in facilitiesListUpdated:
                    if(h[0].replace(
                       ' ', '').replace(
                       '\'', '').lower() == s[1].replace(' ', '').lower() and
                        h[1].replace(
                       ' ', '').replace(
                       '\'', '').lower() == s[2].replace(' ', '').lower()):
                        valuesHotelFacilities += ("('', " + "'" +
                                                  str(h[2]) + "', '" +
                                                  str(s[0]) + "', '" +
                                                  str(h[3]) + "'), ")
            valuesHotelFacilities = valuesHotelFacilities[:-2]
            # print valuesHotelFacilities
            cnx = mysql.connector.connect(user='root',
                                          password='PASSWORD',
                                          host='13.65.193.223',
                                          database='tres_jolie_db')
            cursor = cnx.cursor()
            queryUpdateHotelFacil = ("INSERT INTO hotel_facilities " +
                                     "(id, HotelCode, facility_id, extra_info) " +
                                     "VALUES " + valuesHotelFacilities + " " +
                                     "ON DUPLICATE KEY UPDATE " +
                                     "HotelCode=VALUES(" +
                                     "HotelCode), " +
                                     "facility_id=VALUES(" +
                                     "facility_id), " +
                                     "extra_info=VALUES(" +
                                     "extra_info)")
            cursor.execute(queryUpdateHotelFacil)
            cnx.commit()
            if print_trace:
                print "---------------DONE -------------------"
    else:
        if print_trace:
            print "---------- DONE with Failure ----------"
    cursor.close()
    cnx.close()