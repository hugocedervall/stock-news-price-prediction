import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# When sampling
seed = 12345

# Load data
dataset = pd.read_csv("train_data_percentage", index_col=0)
dataset = dataset.dropna()
dataset = (dataset[(dataset.lang == "en")]).reset_index(drop=True).sort_index()
dataset = dataset.reset_index(drop=True).sort_index()

# Create categories from priceChanges
stepKey = "1stepChange"
threshold = 0.01
dataset.insert(loc=9, column="priceChangeClass", value=1)
dataset.at[dataset[dataset[stepKey] <= (1 - threshold)].index, "priceChangeClass"] = 0
dataset.at[dataset[dataset[stepKey] >= (1 + threshold)].index, "priceChangeClass"] = 2
#dataset.at[dataset[dataset[stepKey] < 1].index, "priceChangeClass"] = 0 # If 2 classes only

# Summaries containing these sentences will be removed
filtered_summary = [
    "No summary available",
    "Full story available on",
    # "..."

]

for filtr in filtered_summary:
    index_containing = dataset[dataset.summary.str.contains(filtr)].index
    print(f"{len(index_containing)} summaries containing '{filtr}'")
    dataset = dataset.drop(index_containing)
dataset = dataset.reset_index(drop=True)

# Remove all duplicate summaries which have the same related company (news affect stocks differentely)
s1 = len(dataset.index)
dataset = dataset.loc[dataset.duplicated(subset=["summary", "related"]) == False]

s2 = len(dataset.index)
dataset = dataset.loc[dataset.duplicated(subset=["headline", "related"]) == False]

s3 = len(dataset.index)

print(f"Duplicate summaries removed: {s1 - s2}")
print(f"Duplicate headlines removed: {s2 - s3}")

# Balance dataset
smallest_amount_data = min(dataset.priceChangeClass.value_counts())

indexes = []
for unique_class in set(dataset.priceChangeClass):
    temp_indexes = dataset.loc[dataset.priceChangeClass == unique_class].sample(smallest_amount_data, random_state=seed).index
    # print(list(temp_indexes))
    indexes += list(temp_indexes)

balanced_dataset = dataset.loc[indexes].sample(frac=1, random_state=seed).reset_index(drop=True)


#balanced_dataset = dataset.reset_index(drop=True)

# Divide data into train/valid/test
val_percent = 0.1
test_percent = 0.2

random_data = True
if (random_data):
    print(f"randomizing order of data")
    validation_data = (balanced_dataset.sample(frac=val_percent, random_state=seed))
    test_data = (balanced_dataset.iloc[balanced_dataset.index.difference(validation_data.index)].sample(frac=test_percent,
                                                                                                        random_state=seed))
    train_data = (
    balanced_dataset.iloc[balanced_dataset.index.difference(test_data.index).difference(validation_data.index)])
else:
    # In order
    print(f"order of data is time")
    data_len = len(balanced_dataset.index)
    test_index = data_len - int(data_len * test_percent)
    valid_index = test_index - int(data_len * val_percent)
    
    test_data = balanced_dataset.iloc[test_index:]
    validation_data = balanced_dataset.iloc[valid_index:test_index]
    train_data = balanced_dataset.iloc[:valid_index]
    
validation_data = validation_data.reset_index(drop=True)
test_data = test_data.reset_index(drop=True)
train_data = train_data.reset_index(drop=True)
    
    
# Create input output pairs for train/valid/test
x_train = np.array(train_data.summary)
y_train = np.array(train_data.priceChangeClass).astype(np.int8)

val_x = np.array(validation_data.summary)
val_y = np.array(validation_data.priceChangeClass)

test_x = np.array(test_data.summary)
test_y = np.array(test_data.priceChangeClass).astype(np.int8)
