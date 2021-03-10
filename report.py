import yaml
import uuid
import numpy as np
import pandas as pd
from interval import Interval

# calculate and map
def genertColumnData(row, column):
    a = eval(column['process'])
    if column.get('interval_map'):
        for intervalkey in column['interval_map']:
            isLowerClosed = True
            isUpperClosed = True
            if intervalkey[0] == '(':
                isLowerClosed = False
            if intervalkey[len(intervalkey)-1] == ')':
                isUpperClosed= False
            intervalRange = intervalkey[1:len(intervalkey)-1].split(',')
            intervalValue = Interval(int(intervalRange[0]),int(intervalRange[1]),lower_closed=isLowerClosed, upper_closed=isUpperClosed) # todo 无上下限支持
            # if intervalRange[0] != '#' and intervalRange[1] != '#':
            #     intervalValue = Interval(int(intervalRange[0]),int(intervalRange[1]),lower_closed=isLowerClosed, upper_closed=isUpperClosed)
            # elif intervalRange[0] == '#' and intervalRange[1]:
            #     if isUpperClosed: 
            #         intervalValue = Interval.less_than_or_equal_to(int(intervalRange[1]))
            #     else:
            #         intervalValue = Interval.less_than(int(intervalRange[1]))
            # elif intervalRange[0] and intervalRange[1] == '#':
            #     if isLowerClosed: 
            #         intervalValue = Interval.greater_than_or_equal_to(int(intervalRange[0]))
            #     else:
            #         intervalValue = Interval.greater_than(int(intervalRange[0]))
            if intervalValue and a in intervalValue:
                return column['interval_map'][intervalkey]
    elif column.get('list_map'):
        for listKey in column['list_map']:
            if listKey.split(',').count(str(a)) > 0:
                return column['list_map'][listKey]
    elif column.get('map'):
        return column.get('map')[a]
    else :
        return a

def caseDict(dictDate, default):
    for k in dictDate:
        if dictDate[k] == True:
            return k
    return default

customFuncMap = {
    'percentile_50': lambda x: np.percentile(x, 50),
    'percentile_95': lambda x: np.percentile(x, 95),
    'percentile_5': lambda x: np.percentile(x, 5)
}

f = open('report.yaml', 'r', encoding='utf-8')
d = yaml.load(f.read(), Loader=yaml.FullLoader)

df = pd.read_excel(io=d['data']['path'])

indexTitle = {}
columnLable = {}
df['g_index'] = '1'
df['u_index'] = df.apply(lambda _: uuid.uuid4(), axis=1) 
for column in d['columns']:
    if column.get('process'):
        df[column['name']] = df.apply(lambda x: genertColumnData(x, column), axis=1)
    if column.get('lable'):
        columnLable[column['name']] = column['lable']
    if column.get('lables'):
        lables = {}
        for index in range(len(column.get('lables'))):
           lables[index+1] = column.get('lables')[index]
        columnLable[column['name']] = lables
    if column.get('title'):
        indexTitle[column['name']] = column['title']
   
result = pd.DataFrame()
for index in d['cross']['indexs']:
    df1 = pd.DataFrame()
    for column in d['cross']['columns']:
        aggfunc = index['aggfunc']
        if customFuncMap.get(aggfunc):
            aggfunc = customFuncMap[aggfunc]

        df2 = pd.DataFrame()
        if index.get('type') and columnLable.get(index['value']):
            for val in columnLable[index['value']]:
                df3 = pd.crosstab(index=df[index['index']],
                            columns=df[column],
                            values=df[val],
                            aggfunc=aggfunc,
                            margins=d['data']['margins']
                        ).round(d['data']['round']).fillna('-')

                df2 = df3 if df2.empty else pd.concat([df2, df3])

                df2 = df2.drop(index = '1')
                df2.rename(index={'All': columnLable[index['value']][val]},inplace = True)

            df2.loc['总计'] = df2.apply(lambda x: x.sum())
        else:
            df2 = pd.crosstab(index=df[index['index']],
                        columns=df[column],
                        values=df[index['value']],
                        aggfunc=aggfunc,
                        margins=d['data']['margins']
                    ).round(d['data']['round']).fillna('-')

            currentIndex = index['index']
            if index['index'] == 'g_index':
                currentIndex = index['value']
                if columnLable.get(currentIndex):
                    columnLable[currentIndex]['All'] = columnLable[currentIndex][1]
                    df2 = df2.drop(index = '1')
            elif columnLable.get(currentIndex):
                columnLable[currentIndex]['All'] = '总计'
                df2 = df2.reindex(index=columnLable[currentIndex]).fillna('-')

            if columnLable.get(currentIndex):
                df2.rename(index=columnLable[currentIndex],inplace = True)

        if columnLable.get(column):
            columnLable[column]['All'] = '总计'
            df2 = df2.reindex(columns=columnLable[column]).fillna('-')
            df2.rename(columns = columnLable[column],inplace = True)

        currentIndex = index['index'] if index['index'] != 'g_index' else index['value']
        df2.insert(0, 'fi', indexTitle[currentIndex])
        df2 = df2.set_index('fi',drop=True, append=True, inplace=False, verify_integrity=False)
        df2 = df2.swaplevel(index['index'],'fi')

        df1 = df2 if df1.empty else pd.concat([df1, df2], join="inner", axis=1)

    result = df1 if result.empty else pd.concat([result, df1])

result.to_excel(d['data']['outpath'])
