import asyncio
from datetime import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta

from pymongo_get_database import get_database

async def aggregate(data: str, collection=None) -> dict or None:
    """Main function, that takes raw string data, validates it, calls 
    db request func and return result"""
    try:
        dt_from = data['dt_from']
        dt_upto = data['dt_upto']
        group_type = data['group_type']
    except KeyError:
        return None
    try:
        dt_from = parser.parse(dt_from)
        dt_upto = parser.parse(dt_upto)
    except ValueError:
        return None
    if not (group_type == 'month' 
            or group_type == 'day' 
            or group_type == 'hour'):
        return None
    
    return await get_aggregation_data(dt_from, dt_upto, group_type, collection)

def get_periods(start: datetime, finish: datetime, period_type) -> list[str]:
    """Auxiliary function that creates list of all consecutive dates 
    according to period type"""
    periods = []
    while start.timestamp() <= finish.timestamp():
        periods.append(start)
        if period_type == "month":
            start += relativedelta(months=+1)
        elif period_type == "day":
            start += relativedelta(days=+1)
        elif period_type == "hour":
            start += relativedelta(hours=+1)

    periods = [x.strftime("%Y-%m-%dT%H:%M:%S") for x in periods]
    return periods

async def get_aggregation_data(dt_from: datetime, dt_upto: datetime, 
                               group_type: str, collection=None ) -> dict:
    """Function that makes request for db and process output data"""
    if collection is None:
        dbname = get_database("cv")
        collection = dbname["sample_collection"]
    match group_type:
        case "month":
            agg_format = "%Y-%m"
        case "day":
            agg_format = "%Y-%m-%d"
        case "hour":
            agg_format = "%Y-%m-%dT%H"
            
    res = collection.aggregate([
        {
            "$match": { 
                "dt": { "$gte": dt_from, "$lte": dt_upto }       
            }
        },
        {
            "$group": {

                "_id": { "$dateToString": { 
                    "date": "$dt", 
                    "format": f"{agg_format}"} 
                },
                "value": { "$sum": "$value"},
            }
        },
        {
            "$sort": { "_id": 1 }
        }
    ])
    
    data = {"dataset": [], "labels": []}
    periods = get_periods(dt_from, dt_upto, group_type)
    
    i = 0
    j = 0
    res = [r for r in res]
    
    while i != len(periods):
        if j == len(res):
            data["dataset"].append(0)
            data['labels'].append(periods[i])
            i += 1
            continue
        if group_type == "month":
            new_date = parser.parse(res[j]['_id'])\
                             .replace(day=1).strftime("%Y-%m-%dT%H:%M:%S")
        else:
            new_date = parser.parse(res[j]['_id'])\
                             .strftime("%Y-%m-%dT%H:%M:%S")
            
        if periods[i] == new_date:
            data['dataset'].append(res[j]['value'])
            data['labels'].append(new_date)
            j += 1
        else:
            data["dataset"].append(0)
            data['labels'].append(periods[i])
        i += 1
                    
    return data

if __name__ == "__main__":
    dt_from = parser.parse("2022-02-01T00:00:00")
    dt_upto = parser.parse("2022-02-02T00:00:00")
    group_type = "hour"
    res = asyncio.run(get_aggregation_data(dt_from, dt_upto, group_type))
    print(res)