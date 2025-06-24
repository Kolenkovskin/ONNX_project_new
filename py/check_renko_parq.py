import pandas as pd
ask_df = pd.read_parquet("C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked\\XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020_test.parquet")
bid_df = pd.read_parquet("C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked\\XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020_test.parquet")
print("Ask tail:\n", ask_df.tail())
print("Bid tail:\n", bid_df.tail())
print("Ask stats:\n", ask_df.describe())
print("Bid stats:\n", bid_df.describe())