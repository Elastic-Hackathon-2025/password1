# app.py (Streamlit UI)
import streamlit as st
import re
import backend
from wordcloud import WordCloud
import matplotlib.pyplot as plt

def main_screen():
    st.title("News Article Recommendations")

    # Input field for user ID
    user_id = st.text_input("Enter your User ID:", "")

    if not user_id:
        st.warning("Please enter a User ID")
        return

    st.write(f"Click to generate news just for you!")

    # A button to trigger the recommendation process
    if st.button('Get Recommendations'):
        st.write(f"Fetching recommendations for You...")
        profile=backend.generate_user_profile2(user_id)

        if profile and "Failed" not in profile:
            st.write("Your interest profile keywords:")
            # Convert comma-separated string to list of words
            keywords = [word.strip() for word in profile.split(',')]
            # Create and display word cloud

            # Generate word cloud
            wordcloud = WordCloud(width=800, height=400, background_color='white').generate(' '.join(keywords))

            # Display the word cloud using matplotlib
            plt.figure(figsize=(10, 5))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis('off')
            st.pyplot(plt)

            st.write("Finding relevant news articles based on your profile...")
            search_results = backend.search_elasticsearch_by_keywords(profile, backend.cloud_id, backend.es_username, backend.es_password)

            if search_results:
                st.write("Here are some news articles that match your interests:")
                # Create columns for articles, 2 articles per row
                for i in range(0, len(search_results), 2):
                    col1, col2 = st.columns(2)

                    with col1:
                        st.write("---")
                        st.write(search_results[i])

                    with col2:
                        if i + 1 < len(search_results):
                            st.write("---")
                            st.write(search_results[i + 1])
            else:
                st.write("No matching news articles found.")
        else:
            st.error("Failed to generate user profile. Please try again.")

# Main Flow of the App
def main():
    main_screen()

# Run the main function
if __name__ == "__main__":
    main()
