from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer


def process_text(df):
    # Tokenization
    tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")

    # Remove stop words
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")

    # Create pipeline
    df = tokenizer.transform(df)
    df = remover.transform(df)

    return df
