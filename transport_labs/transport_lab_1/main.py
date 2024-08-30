import csv
import math

import pymongo as pm
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import gmplot

COLLECTION_NAMES = ['ActiveBookings', 'ActiveParkings', 'PermanentBookings', 'PermanentParkings',
                    'enjoy_ActiveBookings', 'enjoy_ActiveParkings', 'enjoy_PermanentBookings',
                    'enjoy_PermanentParkings']

CITIES = ['New York City', 'Milano']
CITIES_ENJOY = 'Milano'

start_date = datetime(2017, 1, 1, 0, 0, 0)
end_date = datetime(2017, 2, 28, 23, 59, 59)

collection_car2go = ["PermanentBookings", "PermanentParkings"]
collection_enjoy = ['enjoy_PermanentBookings', 'enjoy_PermanentParkings']


def setup_connection():
    client = pm.MongoClient('bigdatadb.polito.it',
                            ssl=True,
                            authSource='carsharing',
                            tlsAllowInvalidCertificates=True)
    db_car_sharing = client['carsharing']
    db_car_sharing.authenticate('ictts', 'Ictts16!')
    return db_car_sharing


def count_collections(db):
    for name in COLLECTION_NAMES:
        collection_count = str(db.get_collection(name).estimated_document_count())
        print(name + ": " + collection_count)


def print_cities(db):
    cities = db.get_collection('PermanentBookings').distinct('city')
    enjoy_cities = db.get_collection('enjoy_PermanentBookings').distinct('city')
    print(f'Cities: {cities}')
    print(f'Enjoy_Cities: {enjoy_cities}')


def duration_of_data(db):
    timestamp_enj = db.get_collection('enjoy_PermanentBookings').find().sort('init_time', pm.ASCENDING).distinct(
        'init_time')
    start_timestamp_enj = int(timestamp_enj[-1])
    end_timestamp_enj = int(timestamp_enj[0])

    timestamp = db.get_collection('PermanentBookings').find().sort('init_time', pm.ASCENDING).distinct('init_time')
    start_timestamp = int(timestamp[-1])
    end_timestamp = int(timestamp[0])

    print("Car2Go collection started time {}".format
          (datetime.utcfromtimestamp(start_timestamp).strftime('%Y−%m−%d %H:%M:%S')))
    print("Car2Go collection ended time {}".format(
        datetime.utcfromtimestamp(end_timestamp).strftime('%Y−%m−%d %H:%M:%S')))
    print("Enjoy collection started time {}".format(
        datetime.utcfromtimestamp(start_timestamp_enj).strftime('%Y−%m−%d %H:%M:%S')))
    print("Enjoy collection ended time {}".format(
        datetime.utcfromtimestamp(end_timestamp_enj).strftime('%Y−%m−%d %H:%M:%S')))

    first_timestamp = db.get_collection('PermanentBookings').find_one({'init_time': start_timestamp})
    date = first_timestamp.get('init_date')
    city = first_timestamp.get('city')
    print("Car2Go local time of first timestamp {} in {}".format(date, city))

    first_timestamp_enj = db.get_collection('enjoy_PermanentBookings').find_one({'init_time': start_timestamp_enj})
    date_enj = first_timestamp_enj.get('init_date')
    city_enj = first_timestamp.get('city')
    print("Enjoy local time of first timestamp {} in {}".format(date_enj, city_enj))


def amount_of_cars(db):
    # car2go
    for city in CITIES:
        cars = db.get_collection('PermanentBookings').distinct('plate', {'city': city})
        print('Car2Go cars in {}: {}'.format(city, len(cars)))
    # enjoy
    cars_enjoy = db.get_collection('enjoy_PermanentBookings').distinct('plate', {'city': CITIES_ENJOY})
    print('Enjoy cars in {}: {}'.format(CITIES_ENJOY, len(cars_enjoy)))


def bookings_on_november(db):
    start_time = datetime(2017, 11, 1, 0, 0, 0)
    end_time = datetime(2017, 11, 30, 23, 59, 59)
    for city in CITIES:
        total_bookings = db.get_collection('PermanentBookings').count_documents({'$and': [
            {'city': city},
            {'init_date': {'$gte': start_time}},
            {'final_date': {'$lte': end_time}}
        ]})
        print('Car2go bookings in {}: {}'.format(city, total_bookings))

    total_bookings_enjoy = db.get_collection('enjoy_PermanentBookings').count_documents({'$and': [
        {'city': CITIES_ENJOY},
        {'init_date': {'$gte': start_time}},
        {'final_date': {'$lte': end_time}}
    ]})
    print('Enjoy bookings in {}: {}'.format(CITIES_ENJOY, total_bookings_enjoy))


def alternatives(db):
    for city in CITIES:
        total = db.get_collection('PermanentBookings').count_documents(
            {'$and': [{'city': city},
                      {'$or': [
                          {'walking.distance': {'$ne': -1}},
                          {'driving.distance': {'$ne': -1}},
                          {'public_transport.distance': {'$ne': -1}},
                      ]
                      }
                      ]
             }
        )
        print('Car2go alternatives in {}: {}'.format(city, total))
    total_enj = db.get_collection('enjoy_PermanentBookings').count_documents(
        {'$and': [{'city': CITIES_ENJOY},
                  {'$or': [
                      {'walking.distance': {'$ne': -1}},
                      {'driving.distance': {'$ne': -1}},
                      {'public_transport.distance': {'$ne': -1}},
                  ]
                  }
                  ]
         }
    )
    print('Enjoy alternatives in {}: {}'.format(CITIES_ENJOY, total_enj))


# 1.0
def pre_data_analysis(db):
    count_collections(db)
    print_cities(db)
    duration_of_data(db)
    amount_of_cars(db)
    bookings_on_november(db)
    alternatives(db)


def period_of_date():
    start = datetime.strptime("01-01-2017", "%d-%m-%Y")
    end = datetime.strptime("28-02-2017", "%d-%m-%Y")
    return [start + timedelta(days=x) for x in range(0, (end - start).days + 1, 2)]


def pipline_durations(db, collection, city, start_date, end_date):
    return list(db.get_collection(collection).aggregate(
        [
            {  # filter data for a given city in a given period
                '$match': {
                    '$and': [
                        {'city': city},
                        {'init_date': {'$gte': start_date}},
                        {'final_date': {'$lte': end_date}}
                    ]}
            },
            {  # calculate duration of booking and parking: duration = (final_time-init_time)/60
                '$project': {
                    '_id': 1,
                    'city': 1,
                    'duration': {'$divide': [
                        {
                            '$subtract': ['$final_time', '$init_time']
                        }, 60
                    ]
                    }
                }
            },
            {  # group by duration and calculate sum of unique durations
                '$group': {
                    '_id': '$duration', # 10m, 11m
                    'total_rentals': {'$sum': 1} # 5, 7
                }
            },
            {  # sort by id(duration)
                '$sort': {'_id': 1}
            }
        ]
    ))


def pipline_durations_days(db, collection, city, start_date, end_date, day):
    return list(db.get_collection(collection).aggregate(
        [
            {  # filter data for a given city in a given period
                '$match': {
                    '$and': [
                        {'city': city},
                        {'init_date': {'$gte': start_date}},
                        {'final_date': {'$lte': end_date}}
                    ]}
            },
            {  # calculate duration of booking and parking: duration = (final_time-init_time)/60 | added weekday
                '$project': {
                    '_id': 1,
                    'city': 1,  # 3                    # '12-10-2022'
                    'weekday': {'$isoDayOfWeek': '$init_date'},
                    'duration': {'$divide': [
                        {
                            '$subtract': ['$final_time', '$init_time']
                        }, 60
                    ]
                    }
                }
            },
            {  # select by given weekday
                '$match': {'weekday': day}
            },
            {  # group by duration and calculate sum of unique durations
                '$group': {
                    '_id': '$duration',
                    'total_rentals': {'$sum': 1}
                }
            },
            {  # sort by id(duration)
                '$sort': {'_id': 1}
            }
        ]
    ))


def pipline_cars_per_hour_unfiltered(db, collection, city, start_date, end_date):
    return list(db.get_collection(collection).aggregate(
        [
            {  # filter data for a given city in a given period
                '$match': {
                    '$and': [
                        {'city': city},
                        {'init_date': {'$gte': start_date}},
                        {'final_date': {'$lte': end_date}}
                    ]}
            },
            {  # convert init_date to part of date like:
                # date_parts: [{year: 2017}, {month: 1}, {day: 1}, {hour: 11}, {minute: 20}]
                '$project': {
                    '_id': 1,
                    'city': 1,
                    'date_parts': {'$dateToParts': {'date': '$init_date'}}
                }
            },
            {  # group by month,day,hour and calculate sum
                '$group': {
                    '_id': {
                        'month': '$date_parts.month',
                        'day': '$date_parts.day',
                        'hour': '$date_parts.hour'
                    },
                    'total_rentals': {'$sum': 1}
                }
            },
            {  # sort by id(month,day,hour)
                '$sort': {'_id': 1}
            }
        ]
    ))


def pipline_cars_per_hour_filtered(db, collection, city, start_date, end_date):
    condition_booking = {
        '$cond': [
            {'$ne': ['$origin_destination', 'undefined']},
            {'$ne': [  # match the coordinates
                {'$arrayElemAt': ['$origin_destination.coordinates', 0]},
                {'$arrayElemAt': ['$origin_destination.coordinates', 1]}
            ]},
            True
        ]
    }
    condition_parking = {
        '$cond': [  # condition for identifying is car moved or not
            {'$ne': ['$loc', 'undefined']},
            {'$ne': [  # match the coordinates
                {'$arrayElemAt': ['$loc.coordinates', 0]},
                {'$arrayElemAt': ['$loc.coordinates', 1]}
            ]},
            True
        ]
    }
    # choice correct condition according to collection
    condition = condition_booking if collection == 'PermanentBookings' or collection == 'enjoy_PermanentBookings' else condition_parking

    return list(db.get_collection(collection).aggregate(
        [
            {  # filter data for a given city in a given period
                '$match': {
                    '$and': [
                        {'city': city},
                        {'init_date': {'$gte': start_date}},
                        {'final_date': {'$lte': end_date}}
                    ]}
            },
            {  # convert init_date to part of date like:
                # date_parts: [{year: 2017}, {month: 1}, {day: 1}, {hour: 11}, {minute: 20}]
                # moved status
                # calculate duration of booking and parking: duration = (final_time-init_time)/60 | added weekday
                '$project': {
                    '_id': 1,
                    'city': 1,
                    'moved': condition,
                    'duration': {
                        '$divide': [
                            {
                                '$subtract': ['$final_time', '$init_time'] # 899
                            },
                            60
                        ]
                        # 15minut
                    },
                    'date_parts': {'$dateToParts': {'date': '$init_date'}}
                }
            },
            {  # filter the data
                '$match': {
                    '$and': [
                        {'duration': {'$gte': 3}},
                        {'duration': {'$lte': 180}},
                        {'moved': True}
                    ]
                }
            },
            {  # calculate sum of rentals per hour on filtered data
                '$group': {
                    '_id': {
                        'month': '$date_parts.month',
                        'day': '$date_parts.day',
                        'hour': '$date_parts.hour'
                    },
                    'total_rentals': {'$sum': 1}
                }
            },
            {  # sort by id(month,day,hour)
                '$sort': {'_id': 1}
            }
        ]
    ))


def pipline_avg_duration_per_day(db, collection, city, start_date, end_date):
    condition_booking = {
        '$cond': [
            {'$ne': ['$origin_destination', 'undefined']},
            {'$ne': [
                {'$arrayElemAt': ['$origin_destination.coordinates', 0]},
                {'$arrayElemAt': ['$origin_destination.coordinates', 1]}
            ]},
            True
        ]
    }
    condition_parking = {
        '$cond': [
            {'$ne': ['$loc', 'undefined']},
            {'$ne': [
                {'$arrayElemAt': ['$loc.coordinates', 0]},
                {'$arrayElemAt': ['$loc.coordinates', 1]}
            ]},
            True
        ]
    }

    condition = condition_booking if collection == 'PermanentBookings' or collection == 'enjoy_PermanentBookings' else condition_parking

    return list(db.get_collection(collection).aggregate(
        [
            {  # filter data for a given city in a given period
                '$match': {
                    '$and': [
                        {'city': city},
                        {'init_date': {'$gte': start_date}},
                        {'final_date': {'$lte': end_date}}
                    ]}
            },
            {  # convert init_date to part of date like:
                # date_parts: [{year: 2017}, {month: 1}, {day: 1}, {hour: 11}, {minute: 20}]
                # moved status
                # calculate duration of booking and parking: duration = (final_time-init_time)/60 | added weekday
                '$project': {
                    '_id': 1,
                    'city': 1,
                    'moved': condition,
                    'duration': {
                        '$divide': [
                            {
                                '$subtract': ['$final_time', '$init_time']
                            },
                            60
                        ]
                    },
                    'date_parts': {'$dateToParts': {'date': '$init_date'}}
                }
            },
            {  # filter the data
                '$match': {
                    '$and': [
                        {'duration': {'$gte': 3}},
                        {'duration': {'$lte': 180}},
                        {'moved': True}
                    ]
                }
            },
            {  # calculate average and standard deviation duration per day on filtered data
                '$group': {
                    '_id': {
                        'month': '$date_parts.month',
                        'day': '$date_parts.day'
                    },
                    'avg_duration': {'$avg': '$duration'},
                    'std_duration': {'$stdDevPop': '$duration'},
                    'durations': {'$push': '$duration'}
                }
            },
            {  # sort by id(month,day)
                '$sort': {'_id': 1}
            }
        ]
    ))


def pipline_parking_per_hour(db, collection, city, start_date, end_date):
    return list(db.get_collection(collection).aggregate(
        [
            {  # filter data for a given city in a given period
                '$match': {
                    '$and': [
                        {'city': city},
                        {'init_date': {'$gte': start_date}},
                        {'final_date': {'$lte': end_date}}
                    ]}
            },
            {
                "$project": {
                    "_id": 1,
                    "init_long": {"$arrayElemAt": [
                        "$loc.coordinates", 0
                    ]},
                    "init_lat": {"$arrayElemAt": [
                        "$loc.coordinates", 1
                    ]},
                    "final_long": {"$arrayElemAt": [
                        "$loc.coordinates", 0
                    ]},
                    "final_lat": {"$arrayElemAt": [
                        "$loc.coordinates", 1
                    ]},
                    "moved": {
                        "$ne": [
                            {"$arrayElemAt": ["$loc.coordinates", 0]},
                            {"$arrayElemAt": ["$loc.coordinates", 1]}
                        ]
                    },
                    "duration": {
                        "$divide": [
                            {
                                "$subtract": ["$final_time", "$init_time"]
                            },
                            60
                        ]
                    }
                }
            },
            {  # filter the data
                '$match': {
                    '$and': [
                        {'duration': {'$gte': 3}},
                        {'duration': {'$lte': 180}},
                        {'moved': True}
                    ]
                }
            }
        ]
    ))


def pipline_min_max_coordinate(db, collection, city, start_date, end_date):
    condition_booking = {
        '$ne': [
            {'$arrayElemAt': ['$origin_destination.coordinates', 0]},
            {'$arrayElemAt': ['$origin_destination.coordinates', 1]}
        ]
    }
    condition_parking = {
        '$ne': [
            {'$arrayElemAt': ['$loc.coordinates', 0]},
            {'$arrayElemAt': ['$loc.coordinates', 1]}
        ]
    }

    condition = condition_booking if collection == 'PermanentBookings' or collection == 'enjoy_PermanentBookings' else condition_parking

    return list(db.get_collection(collection).aggregate(
        [
            {  # filter data for a given city in a given period
                '$match': {
                    '$and': [
                        {'city': city},
                        {'init_date': {'$gte': start_date}},
                        {'final_date': {'$lte': end_date}}
                    ]}
            },
            {
                '$project': {
                    '_id': 1,
                    'loc': 1,
                    'origin': {
                        '$arrayElemAt': [
                            '$origin_destination.coordinates', 0
                        ]
                    },
                    'destination': {
                        '$arrayElemAt': [
                            '$origin_destination.coordinates', 1
                        ]
                    },
                    'moved': condition,
                    'duration': {
                        '$divide': [
                            {
                                '$subtract': ['$final_time', '$init_time']
                            },
                            60
                        ]
                    }
                }
            },
            {  # filter the data
                '$match': {
                    '$and': [
                        {'duration': {'$gte': 3}},
                        {'duration': {'$lte': 180}},
                        {'moved': True}
                    ]
                }
            },
            {  # calculate average and standard deviation duration per day on filtered data
                '$group': {
                    '_id': '$date_parts.hour',
                    'count': {'$sum': 1},
                    'init_min_long': {'$min': {'$arrayElemAt': ['$origin', 0]}},
                    'init_min_lat': {'$min': {'$arrayElemAt': ['$origin', 1]}},
                    'final_min_long': {'$min': {'$arrayElemAt': ['$destination', 0]}},
                    'final_min_lat': {'$min': {'$arrayElemAt': ['$destination', 1]}},
                }
            }
        ]
    ))


def pipline_booking_od(db, collection, city, start_date, end_date):
    return list(db.get_collection(collection).aggregate(
        [
            {  # filter data for a given city in a given period
                '$match': {
                    '$and': [
                        {'city': city},
                        {'init_date': {'$gte': start_date}},
                        {'final_date': {'$lte': end_date}}
                    ]}
            },
            {
                "$project": {
                    "origin": {
                        "$arrayElemAt": ["$origin_destination.coordinates", 0]
                    },
                    "destination": {
                        "$arrayElemAt": ["$origin_destination.coordinates", 1]
                    },
                    "moved": {
                        "$ne": [
                            {"$arrayElemAt": ["$origin_destination.coordinates", 0]},
                            {"$arrayElemAt": ["$origin_destination.coordinates", 1]}
                        ]
                    },
                    "duration": {
                        "$divide": [
                            {
                                "$subtract": ["$final_time", "$init_time"]
                            },
                            60
                        ]
                    }
                }
            },
            {  # filter the data
                '$match': {
                    '$and': [
                        {'duration': {'$gte': 3}},
                        {'duration': {'$lte': 180}},
                        {'moved': True}
                    ]
                }
            },
            {
                "$project": {
                    "origin": 1,
                    "destination": 1
                }
            }
        ]
    ))


def cdf(db):
    # no enjoy data in Milano
    for city in CITIES:
        for collection in collection_car2go:
            # get duration and total rent
            durations = pipline_durations(db, collection, city, start_date, end_date)

            # create list of id and total rent
            duration = [item['_id'] for item in durations] # [1m,2m,3m,4m,5m]
            totrentals = [item['total_rentals'] for item in durations] # [3,4,6,7,3]

            # sum of total rents
            sum_totalrentals = sum(totrentals)

            # calc cumulative total rents
            cumulative_totrentals = [totrentals[0]]
            [cumulative_totrentals.append(cumulative_totrentals[-1] + totrentals[i]) for i in
             range(1, len(totrentals))]
            cumulative_totrentals = [item / sum_totalrentals for item in cumulative_totrentals]

            plt.plot(duration, cumulative_totrentals, label=collection[len(collection) - 8:len(collection)])
            plt.xscale('log')
            plt.xlabel('Duration (min)')
            plt.ylabel('CDF')
        plt.title(city)
        plt.grid()
        plt.legend(loc="upper left")
        plt.show()

    days = [1, 2, 3, 4, 5, 6, 7]
    days_label = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    for city in CITIES:
        for collection in collection_car2go:
            for day in days:
                durations_day = pipline_durations_days(db, collection, city, start_date, end_date, day)

                # create list of id and total rent
                duration = [item['_id'] for item in durations_day]
                totrentals = [item['total_rentals'] for item in durations_day]

                # sum of total rents
                sum_totalrentals = sum(totrentals)

                # calc cumulative total rents
                cumulative_totrentals = [totrentals[0]]
                [cumulative_totrentals.append(cumulative_totrentals[-1] + totrentals[i]) for i in
                 range(1, len(totrentals))]
                cumulative_totrentals = [item / sum_totalrentals for item in cumulative_totrentals]

                plt.plot(duration, cumulative_totrentals, label=collection[len(collection) - 8:len(collection)])
                plt.xscale('log')
                plt.xlabel('Duration (min)')
                plt.ylabel('CDF')
            plt.title(f'{city} - CDF for weekday {collection}')
            plt.grid()
            plt.legend(days_label, loc="upper left")
            plt.show()


def system_utilization(db):
    date_by_day = period_of_date()

    for city in CITIES:
        for collection in collection_car2go:
            # get unfiltered data
            car_per_hour_unfiltired = pipline_cars_per_hour_unfiltered(db, collection, city, start_date, end_date)

            date_by_hour_unfiltered = [
                datetime(2017, item['_id']['month'], item['_id']['day']) + timedelta(hours=item['_id']['hour']) for
                item in car_per_hour_unfiltired]
            totrentals_unfiltired = [item['total_rentals'] for item in car_per_hour_unfiltired]

            plt.plot(date_by_hour_unfiltered, totrentals_unfiltired, color='orange', label='unfiltered')

            # get filtered data
            car_per_hour_filtired = pipline_cars_per_hour_filtered(db, collection, city, start_date, end_date)

            date_by_hour_filtered = [
                datetime(2017, item['_id']['month'], item['_id']['day']) + timedelta(hours=item['_id']['hour']) for
                item in car_per_hour_filtired]
            totrentals_filtired = [item['total_rentals'] for item in car_per_hour_filtired]

            plt.plot(date_by_hour_filtered, totrentals_filtired, color='green', label='filtered')

            plt.xticks(date_by_day, rotation=90)
            plt.xlabel('Day of January and February 2017')
            plt.ylabel('Number of {}'.format(collection))
            plt.grid()
            plt.legend()
            plt.title('Number of {} per hour in {}'.format(collection, city))
            plt.show()


def rental_statistics(db):
    date_by_day = period_of_date()

    for city in CITIES:
        for collection in collection_car2go:
            avg_per_day = pipline_avg_duration_per_day(db, collection, city, start_date, end_date)
            print(avg_per_day[0:10])
            date_duration = [datetime(2017, item['_id']['month'], item['_id']['day']) for item in avg_per_day]
            avg_duration = [item['avg_duration'] for item in avg_per_day]
            std_duration = [item['std_duration'] for item in avg_per_day]
            median_duration = [np.median(item['durations']) for item in avg_per_day]
            percentile_duration = [np.percentile(item['durations'], 85) for item in avg_per_day]

            plt.figure()
            plt.plot(date_duration, avg_duration, color='orange', label='average')
            plt.plot(date_duration, std_duration, color='green', label='standart deviation')
            plt.plot(date_duration, median_duration, color='red', label='median')
            plt.plot(date_duration, percentile_duration, color='blue', label='85 percentile')

            plt.xticks(date_by_day, rotation=90)
            plt.xlabel('Day of January and February 2017')
            plt.ylabel('Minutes')
            plt.grid()
            plt.legend()
            plt.title('Statistics for {} per day in {}'.format(collection[-8:], city))
            plt.show()


def rental_location(db):
    # weekday 6-18 18-6 weekend day 6-18 18-6
    dates = [
        {"period": "weekday_morning", "time": [datetime(2017, 1, 10, 6, 0, 0), datetime(2017, 1, 10, 18, 0, 0)]},
        {"period": "weekday_evening", "time": [datetime(2017, 1, 10, 18, 0, 0), datetime(2017, 1, 11, 6, 0, 0)]},
        {"period": "weekend_morning", "time": [datetime(2017, 1, 14, 6, 0, 0), datetime(2017, 1, 14, 18, 0, 0)]},
        {"period": "weekend_evening", "time": [datetime(2017, 1, 14, 18, 0, 0), datetime(2017, 1, 15, 6, 0, 0)]}
    ]

    for date in dates:
        parking_per_hour = pipline_parking_per_hour(db, collection_car2go[1], 'Milano', date["time"][0],
                                                    date["time"][1])
        for item in parking_per_hour:
            print(item)

        with open(f'{date["period"]}_parking.csv', 'w') as fp:
            fp.write('latitude,longitude')
            for item in parking_per_hour:
                fp.write(
                    '\n{},{}'.format(item['init_lat'], item['init_long']))


def generate_heatmap(parking, density, zone_coordinates):
    apikey = 'AIzaSyDP_MryIaPjHs2CKssrqFCMl40jkWJ7_jw'

    centre_lat = 45.4773
    centre_long = 9.1815
    gmap = gmplot.GoogleMapPlotter(centre_lat, centre_long, 12, apikey=apikey)
    z = 0

    for i in range(density.shape[0]):
        for j in range(density.shape[1]):
            if density[i][j] == 0:
                color = 'white'
            if 0 < density[i][j] <= 15:
                color = '#fff33b'
            if 15 < density[i][j] <= 30:
                color = '#fdc70c'
            if 30 < density[i][j] <= 45:
                color = '#f3903f'
            if 45 < density[i][j] <= 60:
                color = '#ed683c'
            if 60 < density[i][j] <= 75:
                color = '#e93e3a'
            if density[i][j] > 75:
                color = '#7a1204'
            gate = zip(*zone_coordinates[z])

            gmap.polygon(*gate, color=color, edge_width=0.5, face_alpha=0.4)
            z += 1

    gmap.draw(f'{parking}_densities.html')
    print(f'{parking}_densities.html DONE')


def calculate_zones(parking, ):
    df = pd.read_csv(parking + '.csv')

    #
    longitude_variation = 0.006358  # East variation of 500 m long
    latitude_variation = 0.004495  # North variation of 500 m lat

    min_longitude = min(df['longitude'])  # weastern longitude e
    max_longitude = max(df['longitude'])  # eastern longitude e
    min_latitude = min(df['latitude'])  # southern latitude n
    max_latitude = max(df['latitude'])  # northern latitude n

    NS_side = math.ceil((max_latitude - min_latitude) / latitude_variation)  # vertical side of the minimum square cell
    WE_side = math.ceil(
        (max_longitude - min_longitude) / longitude_variation)  # horizontal side of the minimum square cell

    zone_coordinates = []
    density = np.zeros((NS_side, WE_side), dtype=float)

    with open(f"{parking}_heatmap.csv", 'w') as fp:
        grid_writer = csv.writer(fp, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        grid_writer.writerow(['latitude', 'longitude', 'count'])

        for i in range(NS_side):
            north_min = (i * latitude_variation) + min_latitude
            north_max = (i * latitude_variation) + latitude_variation + min_latitude

            for j in range(WE_side):
                east_min = (j * longitude_variation) + min_longitude
                east_max = (j * longitude_variation) + longitude_variation + min_longitude
                count = 0  # count - > number of parkings

                for k in range(1, len(df)):
                    if (east_min < float(df['longitude'][k]) < east_max) & (
                            north_min < float(df['latitude'][k]) < north_max):
                        count += 1

                density[i][j] = count
                grid_writer.writerow([north_min, east_min, count])

                coord = [(north_min, east_min), (north_min, east_max), (north_max, east_max), (north_max, east_min)]
                zone_coordinates.append(coord)

    return zone_coordinates, density


def heatmap(db):
    parking_csv = ['weekday_morning_parking', 'weekday_evening_parking', 'weekend_morning_parking',
                   'weekend_evening_parking']

    for parking in parking_csv:
        zone_coordinates, density = calculate_zones(parking)

        generate_heatmap(parking, density, zone_coordinates)


def od_matrix(db):
    s_date = datetime(2017, 1, 10, 0, 0, 0)
    e_date = datetime(2017, 1, 10, 23, 59, 59)

    city = 'Milano'

    zone_coordinates, density = calculate_zones('weekday_morning_parking')

    trips = pipline_booking_od(db, collection_car2go[0], city, s_date, e_date)

    print('Calculating the travels list through zones...')
    travels = []
    trips_df = pd.DataFrame(trips)

    for trip in range(trips_df.shape[0]):
        for c in range(len(zone_coordinates)):
            if ((trips_df['origin'][trip][0] > zone_coordinates[c][0][1]) &
                    (trips_df['origin'][trip][0] < zone_coordinates[c][1][1]) &
                    (trips_df['origin'][trip][1] > zone_coordinates[c][0][0]) &
                    (trips_df['origin'][trip][1] < zone_coordinates[c][2][0])):
                origin = c
            if ((trips_df['destination'][trip][0] > zone_coordinates[c][0][1]) &
                    (trips_df['destination'][trip][0] < zone_coordinates[c][1][1]) &
                    (trips_df['destination'][trip][1] > zone_coordinates[c][0][0]) &
                    (trips_df['destination'][trip][1] < zone_coordinates[c][2][0])):
                destination = c
        travels.append([origin, destination])

    print('Calculating the OD matrix...')
    OD_matrix = np.zeros((len(zone_coordinates), len(zone_coordinates)), dtype=float)

    for t in travels:2
        for i in range(len(zone_coordinates)):
            for j in range(len(zone_coordinates)):
                if (t[0] == i) & (t[1] == j):
                    OD_matrix[i][j] += 1
    print('Saving the OD matrix...')

    # Save the OD_matrix in a .csv file
    np.savetxt('OD_matrix.csv', OD_matrix, delimiter=',')
    print('Process done')


def vis_od_matrix():
    # Processing the OD matrix
    data = np.loadtxt('OD_matrix.csv', delimiter=',')
    data = np.delete(data, np.where(~data.any(axis=0))[0], axis=1)
    data = np.delete(data, np.where(~data.any(axis=1))[0], axis=0)

    # Check if the new matrix is square
    a = data.shape[0] # rows
    b = data.shape[1] # columns

    if a != b:
        if a > b:
            I = (a - b)
            col = np.zeros((a, I), dtype=float)
            data = np.append(data, col, axis=1)
        else:
            I = (b - a)
            row = np.zeros((I, b), dtype=float)
            data = np.concatenate([data, row], axis=0)

    # Plot the OD matrix
    plt.imshow(data)
    plt.ylabel('Origin')
    plt.xlabel('Destination')
    plt.colorbar()
    plt.show()

    # Plot the circle diagram
    # nodes = []
    # for i in range(len(data[1])):
    #     nodes.append(str(i))
    # plot_connectivity_circle(data, node_names=nodes, facecolor='white', textcolor='black')


if __name__ == '__main__':
    db = setup_connection()

    pre_data_analysis(db)

    cdf(db)

    system_utilization(db)

    rental_statistics(db)
    #
    rental_location(db)
    #
    heatmap(db)
    od_matrix(db)
    # vis_od_matrix()
