1. In the folder 'SQL_Analysis,' you can find my project for the Epam course on SQL Analysis of Google apps:

Google play store apps
In this project I'm going to evaluate different app attributes to decide which characteristics are better perceived by the market. Attributes that's going to be evaluated are app rating, rating count, category. Evaluation parameters are app's average number of reactions (count of rating scores assigned) and / or number installs, and correlation between rating and the percentage of rating count to installs. High number of rating count tells us about the demand for the application and high potential income. High number of rating count tells us about the demand for the application and high potential income indicates that the audience is more loyal to such type of applications. The final result will be a full set of application characteristics, determined by indirect evidence.

My work consisted of several parts:

search for a suitable dataset;
loading the date of the set into the database in denormalized form;
data processing;
development and creation of a database schema;
loading data into the database;
writing SQL queries to the database;
data visualization.

2. In the folder 'PowerBI' you can find my PowerBI reports

3. In the folder 'Course_work' you can find my university course work as a part of the "Database and Application Administration" discipline:

The objective of this course project is to develop a database intended for the "Household Goods Store" application. The application will possess the following functionality:
- User authentication.
- Ability to add products to the cart.
- Ability to remove products from the cart.
- Ability to edit the cart.
- Viewing available products.
- Categorization of products.
A Database Management System (DBMS) is a combination of software and linguistic tools of general or specific purpose that provide management of database creation and utilization.

For the development and management of the course project's database, the Oracle Database 12c relational database management system was used due to its performance and reliability. Additionally, the graphical interface Oracle SQL Developer was employed for its intuitive clarity and broad spectrum of capabilities.

4. In the folder EpamProject you can find the description of the project in Bussiness_template folder

5. In the folder 'pyTeleBot' you can find my latest project. This Telegram bot is designed to assist users in translating words between Polish and Russian languages. It connects to a PostgreSQL database to provide translations when available and allows users to add new word translations to the database. Also! It has a quiz game for language learning and entertainment.
Key Features:

- Translation Service:
	Users can use the /translate command to access the translation service.
	The bot can translate words between Polish and Russian.
	It connects to a PostgreSQL database ("polish") to fetch translations.
- Quiz Game:
	Users can initiate a quiz by using the /quiz command.
	The bot randomly selects a word from its database and provides its Russian translation.
	Users are presented with multiple options for the corresponding Polish translation of the word.
	Users can select an option, and the bot provides feedback on the correctness of their choice.
- Adding Words to the Database:
	When a translation is not found, the bot prompts the user to add the word to the database.
	Users can input the translation, and the bot adds the word and its translation to the database.
- Database Interaction:
	The bot uses a Database class to manage database connections and execute SQL queries.
	It ensures that the database connection is established.
- Inline Keyboard Markup:
	During the quiz, the bot presents answer choices using inline keyboard markup to provide a user-friendly interface.
- Error Handling:
	The bot handles user responses and provides clear messages to guide users through the translation process and quiz game.

Usage Example:
User sends the /translate command.
User inputs a word in Polish or Russian.
The bot fetches the translation from the database and provides it to the user.
If no translation is found, the bot asks the user if they want to