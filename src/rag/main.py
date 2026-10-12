
from src.rag.query_engine import ask_database


def main():
    """Run the air-quality question-answering CLI."""

    print("===================================")
    print(" Bengaluru Air Quality Assistant")
    print("===================================")
    print("Ask a question about air quality.")
    print("Type 'exit' to quit.")

    while True:
        question = input("\nYour question: ").strip()

        if question.lower() == "exit":
            print("Goodbye!")
            break

        if not question:
            print("Please enter a question.")
            continue

        try:
            response = ask_database(question)

            print(f"\nQuestion: {response['question']}")
            print("\nAnswer:")
            print(response["formatted_results"])

        except Exception as error:
            print(f"\nUnable to answer: {type(error).__name__}")
            print("Please try again with a different question.")


if __name__ == "__main__":
    main()
