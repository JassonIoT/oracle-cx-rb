import pandas as pd
import datetime
import IntegracionesCX.base.constants as bc
from datetime import date
from IntegracionesCX.base.helpers import *


class Base:
    def __init__(self, conn):
        self.conn = conn
        self.today = date.today()
        self.plant = bc.THE_PLANT
    
    def production_activity_frozen(self):
        sql_activity_frozen="""
            SELECT A.*, B.NAME, C.PRIMARY_INGREDIENT_ID, D.VARIABLE_QUANTITY*A.QUANTITY*(-1) AS 'QUANTITY_PRIMARY_INGREDIENT',
            E.MATURITY AS 'MATURITY_INGREDIENT', E.SHELF_LIFE AS 'SHELF_LIFE_INGREDIENT', F.MATURITY AS 'MATURITY_FINAL', F.SHELF_LIFE AS 'SHELF_LIFE_FINAL'
            FROM PRODUCTION_ACTIVITY A
            LEFT JOIN MATERIAL B ON substring(A.PROCESS_STEP_ID,7,len(A.PROCESS_STEP_ID)-8)=substring(B.ID, 7, len(B.ID)-6) 
            LEFT JOIN PRODUCTION_PROCESS C ON A.PROCESS_STEP_ID=C.ID 
            LEFT JOIN MATERIAL_PRODUCTION D ON A.PROCESS_STEP_ID=D.PROCESS_STEP_ID AND substring(C.PRIMARY_INGREDIENT_ID,7,len(C.PRIMARY_INGREDIENT_ID)-6)=substring(D.SKU_ID,7,len(D.SKU_ID)-6)
            LEFT JOIN SKU E ON C.PRIMARY_INGREDIENT_ID=E.ID
            LEFT JOIN SKU F ON substring(A.PROCESS_STEP_ID,1,len(A.PROCESS_STEP_ID)-2)=F.ID"""
    
        activity_frozen=pd.read_sql(sql_activity_frozen,self.conn)
        frozen=self.get_frozen_horizon()
        activity_frozen=activity_frozen.loc[activity_frozen['START_TIME']<=frozen]
        activity_frozen.reset_index(inplace=True,drop=True)
        activity_frozen['MACHINE']=activity_frozen['MACHINE_ID'].str.split('~').str[2]
        activity_frozen['BUSINESS_UNIT']=activity_frozen['MACHINE_ID'].str.split('~').str[0]
        activity_frozen['PRIMARY_PRODUCT_ID']=activity_frozen['PROCESS_STEP_ID'].str.split('~').str[1]
        activity_frozen['BOM']=activity_frozen['PROCESS_STEP_ID'].str.split('~').str[2]
        activity_frozen['SKU_ID']=activity_frozen['PROCESS_STEP_ID'].str.split('~').str[0]+'~'+activity_frozen['PROCESS_STEP_ID'].str.split('~').str[1]

        return activity_frozen


    def get_production_activity(self):
        sql_production_activity = """
        SELECT A.*, B.MATURITY AS 'MATURITY_FINAL', B.SHELF_LIFE AS 'SHELF_LIFE_FINAL' FROM (
        SELECT A.*, B.MATURITY AS 'MATURITY_INGREDIENT', B.SHELF_LIFE AS 'SHELF_LIFE_INGREDIENT' FROM (
        SELECT A.*, B.VARIABLE_QUANTITY*A.QUANTITY*(-1) AS 'QUANTITY_PRIMARY_INGREDIENT' FROM (
        SELECT A.*, A.BUSINESS_UNIT+'~'+substring(B.PRIMARY_INGREDIENT_ID,7,len(B.PRIMARY_INGREDIENT_ID)-6) AS 'PRIMARY_INGREDIENT_ID' FROM (
        SELECT A.*, B.NAME FROM (
        SELECT *, A.BUSINESS_UNIT+'~'+PRIMARY_PRODUCT_ID+'~'+BOM AS 'PROCESS_STEP_ID', A.BUSINESS_UNIT+'~'+A.PRIMARY_PRODUCT_ID AS 'SKU_ID' FROM
        (SELECT PAC0.ATTRIBUTE_VALUE AS 'EXTERNAL_ID',PAC1.ATTRIBUTE_VALUE AS 'MACHINE_ID' ,CONVERT(VARCHAR(19), dbo.fn_ConvertToDateTime(cast(PAC2.ATTRIBUTE_VALUE AS BIGINT) / 1000), 121) AS 'START_TIME'
        ,CONVERT(VARCHAR(19), dbo.fn_ConvertToDateTime(cast(PAC3.ATTRIBUTE_VALUE AS BIGINT) / 1000), 121) AS 'END_TIME'        ,PAC4.ATTRIBUTE_VALUE AS 'DURATION' ,PAC5.ATTRIBUTE_VALUE AS 'QUANTITY'
        ,reverse(SUBSTRING(ltrim(reverse(PAC1.ATTRIBUTE_VALUE)), 1, (CHARINDEX('~', ltrim(reverse(PAC1.ATTRIBUTE_VALUE)), 1) - 1))) AS 'MACHINE_ID_X' ,SUBSTRING(PAC1.ATTRIBUTE_VALUE, 1, 5) AS 'BUSINESS_UNIT'
        ,SUBSTRING(PP.PRIMARY_PRODUCT_ID, (CHARINDEX('~', PP.PRIMARY_PRODUCT_ID, 1) + 1), LEN(PP.PRIMARY_PRODUCT_ID)) AS 'PRIMARY_PRODUCT_ID'
        ,reverse(substring(REVERSE(PP.ID), 1, (CHARINDEX('~', REVERSE(PP.ID), 1) - 1))) AS 'BOM','lgolondrino' AS 'OPRID',(SELECT CONVERT(VARCHAR(19), LAST_UPDATE_DATE, 121) FROM PARAMETER) AS 'LAST_UPDATE_DTTM' 
        , PAC7.ATTRIBUTE_VALUE AS 'CHANGEOVER_TIME', PAC8.ATTRIBUTE_VALUE AS 'MACHINE_GROUP'
        FROM PRODUCTION_ACTIVITY_CHANGE PAC0, PRODUCTION_ACTIVITY_CHANGE PAC1 ,PRODUCTION_ACTIVITY_CHANGE PAC2 ,PRODUCTION_ACTIVITY_CHANGE PAC3 ,PRODUCTION_ACTIVITY_CHANGE PAC4 ,PRODUCTION_ACTIVITY_CHANGE PAC5
        ,PRODUCTION_ACTIVITY_CHANGE PAC6, PRODUCTION_ACTIVITY_CHANGE PAC7,PRODUCTION_ACTIVITY_CHANGE PAC8, PRODUCTION_PROCESS PP 
        WHERE PAC1.CHANGE_TYPE = 'INSERTION'
        AND PAC0.ATTRIBUTE_NAME = 'id'
        AND PAC1.ATTRIBUTE_NAME = 'machine'
        AND PAC2.ATTRIBUTE_NAME = 'startTime'
        AND PAC3.ATTRIBUTE_NAME = 'endTime'
        AND PAC4.ATTRIBUTE_NAME = 'duration'
        AND PAC5.ATTRIBUTE_NAME = 'quantity'
        AND PAC6.ATTRIBUTE_NAME = 'processStep'
        AND PAC7.ATTRIBUTE_NAME = 'changeoverTime'
        AND PAC8.ATTRIBUTE_NAME = 'machineGroup'
        AND PAC0.CHANGE_TYPE = PAC1.CHANGE_TYPE
        AND PAC2.CHANGE_TYPE = PAC1.CHANGE_TYPE
        AND PAC3.CHANGE_TYPE = PAC1.CHANGE_TYPE 
        AND PAC4.CHANGE_TYPE = PAC1.CHANGE_TYPE
        AND PAC5.CHANGE_TYPE = PAC1.CHANGE_TYPE 
        AND PAC6.CHANGE_TYPE = PAC1.CHANGE_TYPE
        AND PAC7.CHANGE_TYPE = PAC1.CHANGE_TYPE
        AND PAC8.CHANGE_TYPE = PAC1.CHANGE_TYPE
        AND PAC0.ID = PAC1.ID
        AND PAC2.ID = PAC1.ID
        AND PAC3.ID = PAC1.ID
        AND PAC4.ID = PAC1.ID
        AND PAC5.ID = PAC1.ID
        AND PAC6.ID = PAC1.ID
        AND PAC7.ID = PAC1.ID
        AND PAC8.ID = PAC1.ID
        AND PP.ID = PAC6.ATTRIBUTE_VALUE
        UNION ALL
        SELECT DISTINCT PAC.ID ,isNull(PAC1.ATTRIBUTE_VALUE, (SELECT MACHINE_ID FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID)) AS 'MACHINE_ID',
        CONVERT(VARCHAR(19), isNull(dbo.fn_ConvertToDateTime(cast(PAC2.ATTRIBUTE_VALUE AS BIGINT) / 1000), (SELECT START_TIME FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID)), 121) AS 'START_TIME' ,
        CONVERT(VARCHAR(19), isNull(dbo.fn_ConvertToDateTime(cast(PAC3.ATTRIBUTE_VALUE AS BIGINT) / 1000), (SELECT END_TIME FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID)), 121) AS 'END_TIME',
        isNull(PAC4.ATTRIBUTE_VALUE, (SELECT DURATION FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID)) AS 'DURATION',isNull(PAC5.ATTRIBUTE_VALUE, (SELECT QUANTITY FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID)) AS 'QUANTITY',
        reverse(SUBSTRING(ltrim(reverse(isNull(PAC1.ATTRIBUTE_VALUE, (SELECT MACHINE_ID FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID)))), 1, (CHARINDEX('~', ltrim(reverse(isNull(PAC1.ATTRIBUTE_VALUE, (SELECT MACHINE_ID FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID)))), 1) - 1))) AS 'MACHINE_ID_X'
        ,SUBSTRING((SELECT PROCESS_STEP_ID FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID), 1, 5) AS 'BUSINESS_UNIT'
        ,SUBSTRING((SELECT PROCESS_STEP_ID FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID), (CHARINDEX('~', (SELECT PROCESS_STEP_ID FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID), 1) + 1), LEN((SELECT PROCESS_STEP_ID FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID))-(CHARINDEX('~', (SELECT PROCESS_STEP_ID FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID), 1) + 1)-1) AS 'PRIMARY_PRODUCT_ID'
        ,reverse(substring(REVERSE((SELECT PROCESS_STEP_ID FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID)), 1, (CHARINDEX('~', REVERSE((SELECT PROCESS_STEP_ID FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID)), 1) - 1))) AS 'BOM','lgolondrino' AS OPRID,(SELECT CONVERT(VARCHAR(19), LAST_UPDATE_DATE, 121) FROM PARAMETER) AS 'LAST_UPDATE_DTTM'
        ,isNull(PAC7.ATTRIBUTE_VALUE, (SELECT CHANGEOVER_TIME FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID)) AS 'CHANGEOVER_TIME'
        ,isNull(PAC8.ATTRIBUTE_VALUE, (SELECT MACHINE_GROUP FROM PRODUCTION_ACTIVITY WHERE EXTERNAL_ID = PAC.ID)) AS 'MACHINE_GROUP'
        FROM (((((((PRODUCTION_ACTIVITY_CHANGE PAC LEFT JOIN PRODUCTION_ACTIVITY_CHANGE PAC2 ON (PAC2.ATTRIBUTE_NAME = 'startTime' AND PAC2.CHANGE_TYPE = PAC.CHANGE_TYPE AND PAC2.ID = PAC.ID))
        LEFT JOIN PRODUCTION_ACTIVITY_CHANGE PAC3 ON (PAC3.ATTRIBUTE_NAME = 'endTime' AND PAC3.CHANGE_TYPE = PAC.CHANGE_TYPE AND PAC3.ID = PAC.ID))
        LEFT JOIN PRODUCTION_ACTIVITY_CHANGE PAC4 ON (PAC4.ATTRIBUTE_NAME = 'duration' AND PAC4.CHANGE_TYPE = PAC.CHANGE_TYPE AND PAC4.ID = PAC.ID))
        LEFT JOIN PRODUCTION_ACTIVITY_CHANGE PAC5 ON (PAC5.ATTRIBUTE_NAME = 'quantity' AND PAC5.CHANGE_TYPE = PAC.CHANGE_TYPE AND PAC5.ID = PAC.ID))
        LEFT JOIN PRODUCTION_ACTIVITY_CHANGE PAC1 ON (PAC1.ATTRIBUTE_NAME = 'machine' AND PAC1.CHANGE_TYPE = PAC.CHANGE_TYPE AND PAC1.ID = PAC.ID)) 
        LEFT JOIN PRODUCTION_ACTIVITY_CHANGE PAC7 ON (PAC7.ATTRIBUTE_NAME = 'changeoverTime' AND PAC7.CHANGE_TYPE = PAC.CHANGE_TYPE AND PAC7.ID = PAC.ID))
        LEFT JOIN PRODUCTION_ACTIVITY_CHANGE PAC8 ON (PAC8.ATTRIBUTE_NAME = 'machineGroup' AND PAC8.CHANGE_TYPE = PAC.CHANGE_TYPE AND PAC8.ID = PAC.ID))	
        WHERE PAC.CHANGE_TYPE = 'MODIFICATION' ) A) A
        LEFT JOIN MATERIAL B ON A.PRIMARY_PRODUCT_ID=substring(B.ID, 7, len(B.ID)-6) ) A
        LEFT JOIN PRODUCTION_PROCESS B ON A.PROCESS_STEP_ID=B.ID ) A
        LEFT JOIN MATERIAL_PRODUCTION B ON A.PROCESS_STEP_ID=B.PROCESS_STEP_ID AND substring(A.PRIMARY_INGREDIENT_ID,7,len(A.PRIMARY_INGREDIENT_ID)-6)=substring(B.SKU_ID,7,len(B.SKU_ID)-6) ) A
        LEFT JOIN SKU B ON A.PRIMARY_INGREDIENT_ID=B.ID ) A
        LEFT JOIN SKU B ON A.SKU_ID=B.ID
        """
        production_activity=pd.read_sql(sql_production_activity, self.conn)
        production_activity['DURATION']=pd.to_numeric(production_activity['DURATION'], downcast="float")
        production_activity['DURATION']=production_activity['DURATION']/1440
        production_activity['QUANTITY'] = pd.to_numeric(production_activity['QUANTITY'], downcast="float")
        production_activity['CHANGEOVER_TIME'] = pd.to_numeric(production_activity['CHANGEOVER_TIME'], downcast="float")
        production_activity.rename(columns={'MACHINE_ID_X':'MACHINE'}, inplace=True)
        production_activity.fillna(0, inplace=True)
        production_activity['START_TIME'] = pd.to_datetime(production_activity['START_TIME'])
        production_activity['END_TIME'] = pd.to_datetime(production_activity['END_TIME'])
        
        activity_frozen=self.production_activity_frozen().copy()
    
        for i in range(len(activity_frozen['ID'])):
            one_activity=activity_frozen.loc[i,:]
            
            if one_activity['ID'] not in list(production_activity['EXTERNAL_ID']):
                
                data = {'EXTERNAL_ID':one_activity['ID'], 'MACHINE_ID':one_activity['MACHINE_ID'],'START_TIME':one_activity['START_TIME'],
                        'END_TIME':one_activity['END_TIME'], 'DURATION':one_activity['DURATION'],'QUANTITY':one_activity['QUANTITY'],
                        'MACHINE': one_activity['MACHINE'],'BUSINESS_UNIT':one_activity['BUSINESS_UNIT'], 
                        'PRIMARY_PRODUCT_ID':one_activity['PRIMARY_PRODUCT_ID'],'BOM':one_activity['BOM'],'OPRID':'lgolondrino',
                        'LAST_UPDATE_DTTM':0, 'CHANGEOVER_TIME':one_activity['CHANGEOVER_TIME'],'MACHINE_GROUP':one_activity['MACHINE_GROUP'],
                        'PROCESS_STEP_ID':one_activity['PROCESS_STEP_ID'],'SKU_ID':one_activity['SKU_ID'], 'NAME':one_activity['NAME'],
                        'PRIMARY_INGREDIENT_ID':one_activity['PRIMARY_INGREDIENT_ID'],'QUANTITY_PRIMARY_INGREDIENT':one_activity['QUANTITY_PRIMARY_INGREDIENT'],
                        'MATURITY_INGREDIENT':one_activity['MATURITY_INGREDIENT'],'SHELF_LIFE_INGREDIENT':one_activity['SHELF_LIFE_INGREDIENT'],
                        'MATURITY_FINAL':one_activity['MATURITY_FINAL'],'SHELF_LIFE_FINAL':one_activity['SHELF_LIFE_FINAL']}

                production_activity=production_activity.append(data, ignore_index=True)
        
        production_activity = production_activity.sort_values(by=['START_TIME'], ascending=True).reset_index(drop=True)
        production_activity.fillna(0,inplace=True)
        production_activity.drop_duplicates(inplace=True)
        return production_activity

    def get_pallets(self):
        sql_factor = """SELECT SUBSTRING(muomc.MATERIAL_ID,7,len(muomc.MATERIAL_ID)-5) AS ITEM, muomc.DENOMINATOR,
        (muomc.NUMERATOR / muomc.DENOMINATOR) FACTOR FROM MATERIAL_UOM_CONVERSION muomc WHERE UOM_ID = 'PL'"""
        factor_df = pd.read_sql(sql_factor, self.conn)

        production_activity = self.get_production_activity().copy()
        production_activity = production_activity.merge(factor_df, how='left', left_on='PRIMARY_PRODUCT_ID',
                                                        right_on='ITEM')
        production_activity['QUANTITY_PALLETS'] = production_activity['QUANTITY'] * production_activity['FACTOR']
        lista = []
        for i in range(len(production_activity['QUANTITY_PALLETS'])):
            lista.append(roundBy(production_activity.loc[i, 'QUANTITY_PALLETS']))
        production_activity['QUANTITY_PALLETS_2'] = lista

        production_activity['QUANTITY_PALLET'] = production_activity['DENOMINATOR'] * production_activity[
            'QUANTITY_PALLETS_2']
        # production_activity.to_excel('prueba.xlsx')
        print('get pallets Ok')
        return production_activity

    def get_feature(self):
        sql_feature = """
        SELECT A.FEATURE_ID, A.STATE_ID, A.MACHINE_ID, A.PROCESS_STEP_ID, B.WORK_CENTER_ID 
        FROM PROCESS_STEP_MACHINE_SETUP_STATE A, MACHINE B WHERE B.ID=A.MACHINE_ID
        """
        feature = pd.read_sql(sql_feature, self.conn)
        print('get features Ok')
        return feature

    def get_stock_input(self):
        sql_stock_input = """
        SELECT * FROM STOCK_INPUT 
        """
        stock_input = pd.read_sql(sql_stock_input, self.conn)
        stock_input.fillna('Null', inplace=True)
        print('get stock input Ok')
        return stock_input

    def get_stock_input_primary_ingredients(self):
        production_activity = self.get_production_activity().copy()
        stock_input = self.get_stock_input().copy()
        stock_input = stock_input.rename(columns={'SKU_ID': 'PRIMARY_INGREDIENT_ID'})
        items = production_activity[['PRIMARY_INGREDIENT_ID', 'DURATION']]
        items = items.groupby(by=['PRIMARY_INGREDIENT_ID']).sum()
        items.reset_index(inplace=True)
        # items=items['PRIMARY_INGREDIENT_ID']
        stock_input = items.merge(stock_input, how='left', on='PRIMARY_INGREDIENT_ID')
        stock_input = stock_input.sort_values('AVAILABILITY_DATE', ascending=True).reset_index(drop=True)
        del stock_input['DURATION']
        stock_input.fillna('Null', inplace=True)
        print('get stock input primary ingredients Ok')
        return stock_input

    def get_frozen_horizon(self):
        parameter_sql = """SELECT * FROM PARAMETER"""
        parameter = pd.read_sql(parameter_sql, self.conn)
        parameter = parameter.iloc[0, :]
        start_bucket = pd.to_datetime(parameter['CRRNT_TIME'])
        # start_bucket=pd.to_datetime('2021-18-7 19:00:00')
        frozen_horizon_days = float(parameter['SCHEDULING_FROZEN_HORIZON'] / 60)

        frozen_horizon = start_bucket + datetime.timedelta(days=5)
        frozen_horizon = pd.to_datetime(frozen_horizon)
        print('frozen horizon Ok')
        return frozen_horizon

    def get_dates(self):
        if self.plant == 'Tejas':
            production_activity = self.get_pallets()
        else:
            production_activity = self.get_production_activity().copy()
        frozen_horizon = self.get_frozen_horizon()

        production_activity['MATURITY_DATE'] = ''
        production_activity['SHELF_LIFE_DATE'] = ''
        production_activity['QUANTITY_STOCK_INPUT'] = ''
        production_activity['QUANTITY_STOCK_PRODUCTION'] = ''
        production_activity['UPDATE_QUANTITY'] = ''
        production_activity['ENOUGH_STOCK'] = ''
        production_activity['MISSING'] = ''
        production_activity['FROZEN_HORIZON'] = ''
        production_activity['DIFERENCCE_DAYS'] = production_activity['START_TIME'] - frozen_horizon

        production_activity.loc[production_activity['START_TIME'] <= frozen_horizon, 'FROZEN_HORIZON'] = 1
        production_activity.loc[production_activity['START_TIME'] > frozen_horizon, 'FROZEN_HORIZON'] = 0

        stock_production = production_activity[production_activity['FROZEN_HORIZON'] == 1]
        stock_production = stock_production[
            ['EXTERNAL_ID', 'START_TIME', 'END_TIME', 'QUANTITY', 'SKU_ID', 'MATURITY_FINAL', 'SHELF_LIFE_FINAL']]

        stock_input = self.get_stock_input_primary_ingredients().copy()

        list_update = []
        update_production_activity = pd.DataFrame(list_update, columns=list(production_activity.columns.values))
        for i in range(production_activity['SKU_ID'].size):
            activity = production_activity.loc[i, :].copy()
            activity['DIFERENCCE_DAYS'] = round(activity['DIFERENCCE_DAYS'].total_seconds() / 86400, 2)

            ingredient = activity['PRIMARY_INGREDIENT_ID']

            maturity = activity['MATURITY_INGREDIENT'] / 60
            shelf_life = activity['SHELF_LIFE_INGREDIENT'] / 60

            if maturity != 0 or shelf_life != 0:

                stock_input_ingredient = stock_input[stock_input['PRIMARY_INGREDIENT_ID'] == ingredient].reset_index(
                    drop=True)

                if not stock_input_ingredient.empty:
                    # activity['MATURITY_DATE'] = ''
                    # activity['SHELF_LIFE_DATE'] = ''
                    # activity['QUANTITY_STOCK_INPUT']=''
                    # activity['UPDATE_QUANTITY']=''
                    quantity_ingredient = activity['QUANTITY_PRIMARY_INGREDIENT']

                    for j in range(stock_input_ingredient['PRIMARY_INGREDIENT_ID'].size):
                        stock = stock_input_ingredient.loc[j, :]

                        date = stock['AVAILABILITY_DATE']
                        if date != 'Null':

                            if stock['PRODUCTION_DATE_MAX'] != 'Null':

                                if stock['PRODUCTION_DATE_MAX'] > date:
                                    date = stock['PRODUCTION_DATE_MAX']

                            date_maturity = date + datetime.timedelta(hours=maturity)
                            date_shelf_life = date + datetime.timedelta(hours=shelf_life)

                            activity_start_time = activity['START_TIME']

                            if activity_start_time < date_shelf_life and activity['START_TIME'] > date_maturity:
                                if stock['QUANTITY'] >= quantity_ingredient:
                                    update_stock_quantity = stock['QUANTITY'] - quantity_ingredient
                                    quantity_ingredient = 0
                                    activity['MATURITY_DATE'] = activity['MATURITY_DATE'] + str(date_maturity) + ', '
                                    activity['SHELF_LIFE_DATE'] = activity['SHELF_LIFE_DATE'] + str(
                                        date_shelf_life) + ', '
                                    activity['QUANTITY_STOCK_INPUT'] = activity['QUANTITY_STOCK_INPUT'] + str(
                                        stock['QUANTITY']) + ', '
                                    activity['UPDATE_QUANTITY'] = activity['UPDATE_QUANTITY'] + str(
                                        update_stock_quantity) + ', '
                                    stock_input.loc[
                                        stock_input['ID'] == stock['ID'], 'QUANTITY'] = update_stock_quantity
                                    # activity['MISSING']=0
                                    activity['ENOUGH_STOCK'] = 'Si hay stock'
                                    break
                                else:
                                    update_stock_quantity = 0
                                    quantity_ingredient = quantity_ingredient - stock['QUANTITY']
                                    activity['MATURITY_DATE'] = activity['MATURITY_DATE'] + str(date_maturity) + ', '
                                    activity['SHELF_LIFE_DATE'] = activity['SHELF_LIFE_DATE'] + str(
                                        date_shelf_life) + ', '
                                    activity['QUANTITY_STOCK_INPUT'] = activity['QUANTITY_STOCK_INPUT'] + str(
                                        stock['QUANTITY']) + ', '
                                    activity['UPDATE_QUANTITY'] = activity['UPDATE_QUANTITY'] + str(
                                        update_stock_quantity) + ', '
                                    stock_input.loc[
                                        stock_input['ID'] == stock['ID'], 'QUANTITY'] = update_stock_quantity
                                    # activity['MISSING']=quantity_ingredient
                                    activity['ENOUGH_STOCK'] = 'No hay stock'
                            else:
                                activity['MATURITY_DATE'] = activity['MATURITY_DATE'] + str(date_maturity) + ', '
                                activity['SHELF_LIFE_DATE'] = activity['SHELF_LIFE_DATE'] + str(date_shelf_life) + ', '
                                activity['QUANTITY_STOCK_INPUT'] = activity['QUANTITY_STOCK_INPUT'] + str(
                                    stock['QUANTITY']) + ', '
                                activity['UPDATE_QUANTITY'] = activity['UPDATE_QUANTITY'] + str(
                                    stock['QUANTITY']) + ', '
                                activity['ENOUGH_STOCK'] = 'No hay stock por maduración o shelf life'
                                # activity['MISSING'] = quantity_ingredient

                        else:
                            activity['ENOUGH_STOCK'] = 'No hay stock'

                    activity['MISSING'] = quantity_ingredient
                    if quantity_ingredient != 0 and quantity_ingredient != activity['QUANTITY_PRIMARY_INGREDIENT']:
                        activity['ENOUGH_STOCK'] = 'No hay suficiente stock'
                else:
                    production_activity['ENOUGH_STOCK'] = 'No hay stock'

            else:

                stock_input_ingredient = stock_input[stock_input['PRIMARY_INGREDIENT_ID'] == ingredient].reset_index(
                    drop=True)
                if not stock_input_ingredient.empty:

                    quantity_ingredient = activity['QUANTITY_PRIMARY_INGREDIENT']

                    for j in range(stock_input_ingredient['PRIMARY_INGREDIENT_ID'].size):
                        stock = stock_input_ingredient.loc[j, :]

                        date = stock['AVAILABILITY_DATE']
                        if date != 'Null':

                            if stock['PRODUCTION_DATE_MAX'] != 'Null':

                                if stock['PRODUCTION_DATE_MAX'] > date:
                                    date = stock['PRODUCTION_DATE_MAX']

                            activity_start_time = activity['START_TIME']

                            if date < activity_start_time:
                                if stock['QUANTITY'] >= quantity_ingredient:
                                    update_stock_quantity = stock['QUANTITY'] - quantity_ingredient
                                    quantity_ingredient = 0
                                    activity['QUANTITY_STOCK_INPUT'] = activity['QUANTITY_STOCK_INPUT'] + str(
                                        stock['QUANTITY']) + ', '
                                    activity['UPDATE_QUANTITY'] = activity['UPDATE_QUANTITY'] + str(
                                        update_stock_quantity) + ', '
                                    activity['MATURITY_DATE'] = activity['MATURITY_DATE'] + str(date) + ', '
                                    activity['SHELF_LIFE_DATE'] = activity['SHELF_LIFE_DATE'] + str(date) + ', '
                                    stock_input.loc[
                                        stock_input['ID'] == stock['ID'], 'QUANTITY'] = update_stock_quantity
                                    activity['ENOUGH_STOCK'] = 'Si hay stock'
                                    activity['MISSING'] = 0
                                    break
                                else:
                                    update_stock_quantity = 0
                                    quantity_ingredient = quantity_ingredient - stock['QUANTITY']
                                    activity['QUANTITY_STOCK_INPUT'] = activity['QUANTITY_STOCK_INPUT'] + str(
                                        stock['QUANTITY']) + ', '
                                    activity['UPDATE_QUANTITY'] = activity['UPDATE_QUANTITY'] + str(
                                        update_stock_quantity) + ', '
                                    activity['MATURITY_DATE'] = activity['MATURITY_DATE'] + str(date) + ', '
                                    activity['SHELF_LIFE_DATE'] = activity['SHELF_LIFE_DATE'] + str(date) + ', '
                                    stock_input.loc[
                                        stock_input['ID'] == stock['ID'], 'QUANTITY'] = update_stock_quantity
                                    activity['ENOUGH_STOCK'] = 'No hay stock'
                            else:
                                activity['MATURITY_DATE'] = activity['MATURITY_DATE'] + str(date) + ', '
                                activity['SHELF_LIFE_DATE'] = activity['SHELF_LIFE_DATE'] + str(date) + ', '
                                activity['QUANTITY_STOCK_INPUT'] = activity['QUANTITY_STOCK_INPUT'] + str(
                                    stock['QUANTITY']) + ', '
                                activity['UPDATE_QUANTITY'] = activity['UPDATE_QUANTITY'] + str(
                                    stock['QUANTITY']) + ', '
                                activity['ENOUGH_STOCK'] = 'No hay stock'

                        else:
                            activity['ENOUGH_STOCK'] = 'No hay stock'
                    activity['MISSING'] = quantity_ingredient
                    if quantity_ingredient != 0 and quantity_ingredient != activity['QUANTITY_PRIMARY_INGREDIENT']:
                        activity['ENOUGH_STOCK'] = 'No hay suficiente stock'
                else:
                    production_activity['ENOUGH_STOCK'] = 'No hay stock'

            missing_ingredient = activity['MISSING']
            if missing_ingredient > 0 and activity['FROZEN_HORIZON'] == 0:

                stock_production_ingredient = stock_production[stock_production['SKU_ID'] == ingredient].reset_index(
                    drop=True)
                if not stock_production_ingredient.empty:

                    for n in range(stock_production_ingredient['SKU_ID'].size):

                        stock_production_final = stock_production_ingredient.loc[n, :]
                        maturity_final = stock_production_final['MATURITY_FINAL'] / 60
                        shelf_life_final = stock_production_final['SHELF_LIFE_FINAL'] / 60
                        date_maturity_final = stock_production_final['START_TIME'] + datetime.timedelta(
                            hours=maturity_final)
                        date_shelf_life_final = stock_production_final['END_TIME'] + datetime.timedelta(
                            hours=shelf_life_final)

                        activity_start_time = activity['START_TIME']

                        if activity_start_time >= date_maturity_final and activity_start_time < date_shelf_life_final:

                            if stock_production_final['QUANTITY'] >= missing_ingredient:
                                update_stock_production = stock_production_final['QUANTITY'] - missing_ingredient
                                missing_ingredient = 0
                                activity['QUANTITY_STOCK_PRODUCTION'] = activity['QUANTITY_STOCK_PRODUCTION'] + str(
                                    stock_production_final['QUANTITY']) + ', '
                                stock_production.loc[stock_production['EXTERNAL_ID'] == stock_production_final[
                                    'EXTERNAL_ID'], 'QUANTITY'] = update_stock_production
                                activity['ENOUGH_STOCK'] = 'Si hay stock'
                                activity['MISSING'] = 0
                                break
                            else:
                                update_stock_production = 0
                                missing_ingredient = missing_ingredient - stock_production_final['QUANTITY']
                                activity['QUANTITY_STOCK_PRODUCTION'] = activity['QUANTITY_STOCK_PRODUCTION'] + str(
                                    stock_production_final['QUANTITY']) + ', '
                                stock_production.loc[stock_production['EXTERNAL_ID'] == stock_production_final[
                                    'EXTERNAL_ID'], 'QUANTITY'] = update_stock_production
                                activity['ENOUGH_STOCK'] = 'No hay stock'
                                activity['MISSING'] = missing_ingredient
                else:
                    activity['ENOUGH_STOCK'] = 'No hay stock'

            update_production_activity.loc[i] = list(activity)
        print('get dates Ok')
        return update_production_activity

    def get_base(self):
        print('entro a get base')
        list_final=[]
        production_activity=self.get_dates().copy()
        production_activity = production_activity.sort_values(by=['MACHINE','START_TIME'], ascending=True).reset_index(drop=True)
        work_center_str=production_activity['MACHINE_ID'].str.split('~')
        production_activity['WORK_CENTER_ID'] =work_center_str.str[0]+'~'+work_center_str.str[1]
        production_activity['PRDN_AREA_CODE']=work_center_str.str[1]
        work_center = list(production_activity['WORK_CENTER_ID'].drop_duplicates())
        if self.plant =='Tejas':
            production_activity = production_activity[['BUSINESS_UNIT', 'PRIMARY_PRODUCT_ID', 'NAME', 'BOM', 'PRDN_AREA_CODE', 'MACHINE',
                 'MACHINE_ID', 'MACHINE_GROUP', 'START_TIME', 'END_TIME', 'EXTERNAL_ID', 'PROCESS_STEP_ID', 'QUANTITY',
                 'QUANTITY_PALLET', 'DURATION', 'CHANGEOVER_TIME', 'WORK_CENTER_ID',
                 'PRIMARY_INGREDIENT_ID', 'QUANTITY_PRIMARY_INGREDIENT', 'QUANTITY_STOCK_INPUT',
                 'QUANTITY_STOCK_PRODUCTION', 'MISSING', 'DIFERENCCE_DAYS','DENOMINATOR','FACTOR']]
        else:
            production_activity = production_activity[['BUSINESS_UNIT', 'PRIMARY_PRODUCT_ID', 'NAME', 'BOM', 'PRDN_AREA_CODE', 'MACHINE',
                     'MACHINE_ID', 'MACHINE_GROUP','START_TIME', 'END_TIME', 'EXTERNAL_ID', 'PROCESS_STEP_ID', 'QUANTITY','DURATION', 'CHANGEOVER_TIME', 'WORK_CENTER_ID',
                     'PRIMARY_INGREDIENT_ID','QUANTITY_PRIMARY_INGREDIENT', 'QUANTITY_STOCK_INPUT','QUANTITY_STOCK_PRODUCTION','MISSING','DIFERENCCE_DAYS']]

        feature=self.get_feature()

        for i in range(len(work_center)):
 
            feature_x_work_center=feature.loc[feature['WORK_CENTER_ID']==work_center[i],:]
            if feature_x_work_center.empty:
                name_excel='Bases'+'_'+work_center[i]+'_'+str(self.today)+'.xlsx'
                production_activity.to_excel('/code/app/files/'+name_excel)
            else:
                del feature_x_work_center['WORK_CENTER_ID']
                feature_x_work_center.reset_index(inplace=True, drop=True)
                num_features=list(feature_x_work_center['FEATURE_ID'].drop_duplicates())
                production_activity_work_center=production_activity.loc[production_activity['WORK_CENTER_ID']==work_center[i],:]
                production_activity_work_center.reset_index(inplace=True, drop=True)
                feature_pivot=feature_x_work_center.pivot_table(index= ['MACHINE_ID','PROCESS_STEP_ID'],columns='FEATURE_ID', values=['STATE_ID'], aggfunc=lambda x: ' '.join(x))
                feature_pivot.columns = feature_pivot.columns.droplevel()
                feature_pivot.reset_index(inplace=True)
                feature_pivot.columns = feature_pivot.columns.tolist()

                final = production_activity_work_center.merge(feature_pivot, left_on=['MACHINE_ID', 'PROCESS_STEP_ID'],
                                                      right_on=['MACHINE_ID', 'PROCESS_STEP_ID'], how='left')
                del final['WORK_CENTER_ID']
                name_excel='Bases'+'_'+work_center[i]+'_'+str(self.today)+'.xlsx'
                list_final.append(final)
                final.to_excel('/code/app/files/'+name_excel)

        print('finalizo get base')
        return list_final
