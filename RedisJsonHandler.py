# encoding: utf-8

from redis import Redis


class RedisToJson:

    def __init__(self):
        self.rdb = Redis(host='localhost', port='6379')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.rdb.close()

    @staticmethod
    def clean_result(data):
        if isinstance(data, dict):
            ref = {}
            for _k, _v in data.items():
                ref[_k.decode()] = _v.decode()
            return ref
        else:
            return data

    def get_latest_for_date(self, _date):
        datetime_key = b'max(dateAdded)'

        recent_df_ref = 'recent_df:{}'.format(_date)
        datetime_dict = self.rdb.hgetall(recent_df_ref)

        if not datetime_dict:
            return {}

        base_df_key = datetime_dict[datetime_key].decode()
        latest_data = self.rdb.hgetall('base_df:{}'.format(base_df_key))
        result = self.clean_result(latest_data)
        result['timestamp'] = base_df_key
        return result

    def get_brand_count_by_date(self, _date):
        ref_pattern = 'brand_count:{}_*'.format(_date)

        all_keys = self.rdb.keys(ref_pattern)

        result = {}

        if not all_keys:
            return result

        result[_date] = []

        for each_key in all_keys:
            data = self.rdb.hgetall(each_key)
            result[_date].append({
                'brand': data[b'brand'].decode(),
                'count': data[b'count(brand)'].decode()
            })
        return result

    def get_items_by_color(self, _color):
        _color = _color.strip().lower().replace(' ', '_')
        ref_pattern = 'color_df:*___{}_colors'.format(_color)
        all_keys = self.rdb.keys(ref_pattern)

        result = {}

        if not all_keys:
            return result

        result[_color] = []

        for each_key in all_keys:
            data = self.rdb.hgetall(each_key)
            result[_color].append(
                self.clean_result(data)
            )

        return result
