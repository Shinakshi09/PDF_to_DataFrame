import PyPDF2
import camelot
from collections import namedtuple
import pandas as pd
import pdfplumber
import re
from tabula import read_pdf
from tabula import convert_into
import tabula
from tabula import read_pdf
from tabula import convert_into
import pandas as pd
import numpy as np

df=read_pdf("MAY2021_AA01491168_TXN.pdf",password="AZKPM1806F",pages='all',lattice=True,stream=True)
dataframe_net = {i: df[i] for i in range(0, len(df))} ##### where df is the list  of dataframes

lst = list(dataframe_net.values())
df = pd.concat(lst)

df1=df[['ISIN', 'Security', 'Transaction\rParticulars', 'Date', 'Op. Bal',
       'Credit', 'Debit', 'Cl. Bal', 'Stamp\rDuty\r(`)','Current\rBal', 'Frozen\rBal', 'Pledge\rBal', 'Pledge\rSetup\rBal',
       'Free Bal', 'Market\rPrice /\rFace\rValue', 'Value (`)',
       'Balance Description', 'Lockin', 'Pending\rDemat', 'Pending\rRemat','Scheme Name', 'Folio No.', 'Closing Bal\r(Units)', 'NAV (`)',
       'Cumulative\rAmount\rInvested (in\rINR)', 'Valuation (`)']]
df1=df1.dropna(how="all")

# TRANSACTIONS DATA
a=df1[["ISIN",'Security','Transaction\rParticulars', 'Date', 'Op. Bal',
       'Credit', 'Debit', 'Cl. Bal', 'Stamp\rDuty\r(`)']]
a=a.set_index(["ISIN","Date","Security"])
a=a.dropna(how="all")

# HOLDINGS DATA
b=df1[['ISIN', 'Security', 'Current\rBal', 'Frozen\rBal', 'Pledge\rBal',
       'Pledge\rSetup\rBal', 'Free Bal', 'Market\rPrice /\rFace\rValue',
       'Value (`)']]
b=b.set_index(["ISIN","Security"])
b=b.dropna(how="all")

# HOLDING DATA (OTHER DETAILS)
c=df1[['ISIN', 'Security', 'Date', 'Balance Description', 'Lockin',
       'Pending\rDemat', 'Pending\rRemat']]
c=c.set_index(["ISIN","Security","Date"])
c=c.dropna(how="all")

# MUTUAL FUND DATA
d=df1[['Scheme Name', 'ISIN', 'Folio No.', 'Closing Bal\r(Units)', 'NAV (`)',
       'Cumulative\rAmount\rInvested (in\rINR)', 'Valuation (`)']]
d=d.set_index(["ISIN"])
d=d.dropna(how="all")

# with pd.ExcelWriter ("cdsl.xlsx") as writer:
#     a.to_excel(writer, sheet_name="Sheet_name_1")
#     b.to_excel(writer,sheet_name="Sheet_name_2")
#     c.to_excel(writer,sheet_name="Sheet_name_3")
#     d.to_excel(writer,sheet_name="Sheet_name_4")

# DATA CLEANING
new_a=a.reset_index()
new_a=new_a[["ISIN","Credit"]]

b=b.reset_index()
new_b=b.merge(new_a,on="ISIN",how="left")

new_d=d.rename(columns={'Closing Bal\r(Units)':"Current\rBal"})
new_d=new_d.rename(columns={'NAV (`)':'Market\rPrice /\rFace\rValue'})
new_d=new_d.rename(columns={'Valuation (`)':'Value (`)'})
new_d=new_d.drop(d[['Folio No.','Cumulative\rAmount\rInvested (in\rINR)']],axis=1)
new_d=new_d.reset_index()
# print(new_d)

# ONE FILE ie final data all together
#1.  added credit from a (dataframe) into b(dataframe)
#2. renamed columns closing bal/\r(units), NAV(), Valuation() to current_bal, marketprice/facevalue, value respectively
# ie in combining b and d, (d being added in the existing columns of b by renaming them)
#3. concatenating new_b and new_d row wise
new_b1 = pd.concat([new_b, new_d], axis=0)
# new_b1=new_b1.drop(new_b1.tail(1).index)
print(new_b1)
# new_b1.to_csv("cdsl2.csv")
# with pd.ExcelWriter ("cdsl1.xlsx") as writer:
#     new_b1.to_excel(writer, sheet_name="Sheet_name_1")
#     c.to_excel(writer,sheet_name="Sheet_name_2")



# Table cell contents sometimes overflow into the next row.
# dataframe_net = {i: df[i] for i in range(0, len(df))} ##### where df is the list  of dataframes
#first i declared a dictionary of the nested list of the data frames
# then i made the data frames from each dictionary items in following code
# The resultant data frame was the combination of the data frames in the nested list j

# HOLDINGS AND TRANSACTIONS DATA ALL TOGETHER
# df1=dataframe_net[12].append(dataframe_net[13])
# print(df1.shape)
# df2=dataframe_net[14].append(dataframe_net[15])
# df2=df2.append(dataframe_net[16])
# df2=df2.drop(df2.tail(1).index) # since last row contains header of next dataframe
# df2=df2.append(dataframe_net[25])
# df2=df2.append(dataframe_net[26])
# df2=df2.drop(df2.tail(1).index) # since last row contains header of next dataframe
# df2=df2.drop(df2.columns[[8,9]],axis=1)
# df2=df2.rename(columns={'Current\rBal':'Cl. Bal'})
# print(df2.shape)
# holding_data=df1.merge(df2,on='ISIN',how="outer")
# holding_data=holding_data.dropna(how="all",axis=1)
# # holding_data=holding_data.drop(holding_data.columns[[0,1]],axis=1)
# print(holding_data.shape)

# HOLDING DATA (OTHER DETAILS ALL TOGETHER)
# df3=dataframe_net[17].append(dataframe_net[18])
# df3=df3.append(dataframe_net[19])
# df3=df3.append(dataframe_net[20])
# df3=df3.append(dataframe_net[21])
# df3=df3.drop(df3.tail(1).index)
# df3=df3.append(dataframe_net[27])
# df3
# hold_data_other=df3.drop(df3.tail(1).index)
# hold_data_other.shape

# MUTUAL FUND UNITS HELD AS ON 30-04-2021
# mut_fund=dataframe_net[33].append(dataframe_net[34])
# mut_fund=mut_fund.dropna(how="all",axis=1)
# mut_fund

# SAVING IN 1 EXCELE FILE IN DIFFERENT SHEETS
# with pd.ExcelWriter ("cdsl.xlsx") as writer:
#     holding_data.to_excel(writer, sheet_name="Sheet_name_1")
#     hold_data_other.to_excel(writer,sheet_name="Sheet_name_2")
#     mut_fund.to_excel(writer,sheet_name="Sheet_name_3")



# reading file with PYPDF2
# with open('APR2021_AA01491168_TXN.pdf', mode='rb') as f:
#    reader = PyPDF2.PdfFileReader(f)
#    if reader.isEncrypted:
#        reader.decrypt('AZKPM1806F')
#        print(f"Number of page: {reader.getNumPages()}")
#        print(reader.getDocumentInfo())

#Reading file with pdfplumber
# all_text=''
# with pdfplumber.open('Raghav_another_format.pdf',password="AZKPM1806F") as pdf:
#     for pdf_page in pdf.pages:
#         single_page_text = pdf_page.extract_text()
#         # print(single_page_text)
#         # separate each page's text with newline
#         all_text = all_text + '\n' + single_page_text
#     print(all_text)