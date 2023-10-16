import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller
from lookups import DestinationDatabase, InputTypes, FredEconomicDataWebScrape
from pandas_data_handler import return_data_as_df, return_create_statement_from_df, return_insert_into_sql_statement_from_df


def get_agg_gdp_vs_presidentials_speech_sentiments(db_session, schema_name):
    query = f""" 
    SELECT 
        date,
        gdp,
        pce,
        gpdi,
        netexp,
        gcec,
        impgs ,
        number_of_speeches_per_president,
        average_negative,
        average_neutral ,
        average_positive,
        average_compound
    FROM {schema_name}.agg_quarterly_gdp_vs_presidentials_speech_sentiments;
    """
    df = return_data_as_df(query, InputTypes.SQL, db_session)
    return df


def predict_values_using_arima(df, dependent_variable, independent_variables, arima_order, future_periods):

    dependent_variable = dependent_variable.value
    independent_variables = independent_variables.value
    arima_order = arima_order.value
    future_periods = future_periods.value

    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    df = df[independent_variables]

    # You may need to experiment with order values
    model = ARIMA(df[dependent_variable], order=arima_order)
    result = model.fit()

    forecast = result.get_forecast(steps=future_periods)
    predicted_values = forecast.predicted_mean
    confidence_intervals = forecast.conf_int()
    return predicted_values, confidence_intervals



def process_prediction_results(predicted_values, confidence_intervals,dependent_variable, latest_date):
    predicted_values = pd.DataFrame(predicted_values)
    predicted_values['increment'] = confidence_intervals['increment'] = range(1, len(predicted_values) + 1)
    predicted_values['date'] = confidence_intervals['date'] = latest_date + pd.DateOffset(months=3) * predicted_values['increment']
    del predicted_values['increment']
    del confidence_intervals['increment']

    dependent_variable = dependent_variable.value
    predicted_values.columns = [f'forecasted {dependent_variable}', 'date']
    forecasted_values = pd.merge(
        predicted_values, confidence_intervals, on='date')
    forecasted_values.set_index('date', inplace=True)
    return forecasted_values


def forecast_kpi_using_arima(db_session, df, dependent_variable, independent_variables, arima_order, future_periods, latest_date):

    predicted_values, confidence_intervals = predict_values_using_arima(
        df, dependent_variable, independent_variables, arima_order, future_periods)

    forecasted_values = process_prediction_results(
        predicted_values, confidence_intervals,dependent_variable,latest_date)

    return forecasted_values


def get_forecast_gdp(db_session, schema_name=DestinationDatabase.SCHEMA_NAME):

    df_table_title = []

    schema_name = schema_name.value

    df_gdp = get_agg_gdp_vs_presidentials_speech_sentiments(
        db_session, schema_name)

    latest_date = df_gdp['date'].max()

    df_forecasted_gdp = forecast_kpi_using_arima(
        db_session, df_gdp, FredEconomicDataWebScrape.DEPENDENT_VAR, FredEconomicDataWebScrape.DEPENDENT_INDEPENDENT_VARS, 
        FredEconomicDataWebScrape.ARIMA_ORDER,FredEconomicDataWebScrape.FUTURE_FORECAST_PERIODS, latest_date)

    if len(df_forecasted_gdp):
        table_title = FredEconomicDataWebScrape.FORECAST_TABLE_TITLE.value
        df_table_title.append([table_title, df_forecasted_gdp])

    return df_table_title
