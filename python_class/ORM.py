import sys


class MetaModelBase(type):
    def __new__(mcs, name, bases, attrs):
        """
        如果類名為 Model, 直接產生 class. 若為Model的子類, 則為實際的table,
        將其實例化的欄位名類取出, 並收集於字典 mappings, 最後產生 table class
        :param name:
        :param bases:
        :param attrs:
        """
        if name == 'Model':
            return type.__new__(mcs, name, bases, attrs)
        print('Found model: %s' % name)
        mappings = dict()
        for k, v in attrs.items():
            if isinstance(v, Field):
                # 因為 v 是 Field 物件，因此執行 print 時，會呼叫物件方法 __str__
                print('Found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
        for k in mappings.keys():
            attrs.pop(k)
        attrs['__mappings__'] = mappings  # 保存屬性和列的映射關係
        attrs['__table__'] = name  # 假設表名和類名一致
        return type.__new__(mcs, name, bases, attrs)


# 定義Field類，它負責保存數據庫表的字段名和字段類型
class Field(object):
    def __init__(self, name, column_type):
        # 產生class object時(未實例化)就會改變object的內容(self.name)，原本是指向一個記憶體位置的object
        self.name = name
        self.column_type = column_type

    # 印出物件專屬或是自行設定的訊息
    def __str__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.name)


# 在Field的基礎上，進一步定義各種類型的Field，比如StringField、IntergerField
class StringField(Field):
    def __init__(self, name):
        super().__init__(name, 'varchar(100)')


class IntegerField(Field):
    def __init__(self, name):
        super().__init__(name, 'bigint')


# 與資料庫的交互透過 Model 來實現
class Model(dict, metaclass=MetaModelBase):

    def __init__(self, **kw):
        # 因為 Model 繼承 object， object.__init__只接收一個變數, 初始化參數需要包成字典
        # 所以 Model 若繼承 dict 則可以解決
        # python 3 可以直接用 super().__init__(**kw)
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def save(self):
        fields = []
        params = []
        args = []
        for k, v in self.__mappings__.items():
            fields.append(v.name)
            params.append('?')
            args.append(getattr(self, k, None))
        sql = 'insert into %s (%s) values (%s)' % (self.__table__, ','.join(fields), ','.join(params))
        print('SQL: %s' % sql)
        print('在此修改主要SQL語法')


# 表頭名稱即為類名
class User(Model):
    # 四個欄位名稱與其欄位資料型態
    id = IntegerField('id')
    name = StringField('username')
    email = StringField('email')
    password = StringField('password')


if __name__ == '__main__':
    # 創建一個實例
    u = User(id=12345, name='Michael', email='test@orm.org', password='my-pwd')
    # 保存到數據庫
    u.save()
    sys.exit(0)
