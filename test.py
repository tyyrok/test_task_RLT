from aggregate_query import get_aggregation_data, aggregate
from dateutil import parser
from pytest_asyncio.plugin import pytest

from test_expected_results import expected_result_1, expected_result_2,\
                                                     expected_result_3

@pytest.mark.asyncio
async def test_1():
    dt_from = parser.parse("2022-09-01T00:00:00")
    dt_upto = parser.parse("2022-12-31T23:59:00")
    group_type = "month"
    res = await get_aggregation_data(dt_from, dt_upto, group_type)
    assert  res == expected_result_1

@pytest.mark.asyncio
async def test_2():
    dt_from = parser.parse("2022-10-01T00:00:00")
    dt_upto = parser.parse("2022-11-30T23:59:00")
    group_type = "day"
    res = await get_aggregation_data(dt_from, dt_upto, group_type)
    assert res == expected_result_2
    
@pytest.mark.asyncio
async def test_3():
    dt_from = parser.parse("2022-02-01T00:00:00")
    dt_upto = parser.parse("2022-02-02T00:00:00")
    group_type = "hour"
    res = await get_aggregation_data(dt_from, dt_upto, group_type)
    assert res == expected_result_3
    
@pytest.mark.asyncio
async def test_data_4():
    data = {
        "dt_from": "2022-02-01T00:00:00",
        "dt_upto": "2022-02-02T00:00:00",
        "group_type": "hour"
    }
    res = await aggregate(data)
    assert res == expected_result_3
    
@pytest.mark.asyncio
async def test_incorrect_data_5():
    data = {
        "dt_from": "20dsf0:00:00",
        "dt_upto": "2022-02-02T00:00:00",
        "group_type": "hour"
    }
    res = await aggregate(data)
    assert res == None
    
@pytest.mark.asyncio
async def test_incorrect_data_6():
    data = {
        "dt_upto": "2022-02-02T00:00:00",
        "group_type": "hour"
    }
    res = await aggregate(data)
    assert res == None
    
@pytest.mark.asyncio
async def test_incorrect_data_7():
    data = {
        "dt_from": "2022-02-01T00:00:00",
        "group_type": "hour"
    }
    res = await aggregate(data)
    assert res == None
    
@pytest.mark.asyncio
async def test_incorrect_data_8():
    data = {
        "dt_from": "2022-02-01T00:00:00",
        "dt_upto": "2022-02-02T00:00:00",
    }
    res = await aggregate(data)
    assert res == None