import requests
from bs4 import BeautifulSoup
from detail_log import logger


class PageFetcher:
    """
    This class is responsible for fetching the content from a given URL.
    """

    @staticmethod
    def fetch_page(url: str) -> bytes:
        """
        Fetch the page content from the given URL.

        Args:
            url (string) : Target url to hit the request.

        Returns:
            response.content (Byte) : Response from the request
        """
        try:
            response = requests.get(url)
            response.raise_for_status()  # Ensure valid response
            return response.content
        except requests.exceptions.RequestException:
            logger.error(f"Failed to Established connection with given {url}")
            return None


class MovieParser:
    """
    This class is responsible for parsing movie links from a given filmography page.
    """

    def __init__(self, base_url: str):
        """Initilise the Base Url to parse it

        Args:
            base_url (str): This is base url of filmography url
        """
        self.base_url = base_url

    def parse_movies(self, filmography_url: str) -> list:
        """Extract required details from filmography url and create a list

        Args:
            filmography_url (str): This is Filmography url where we need to hit the request.

        Returns:
            movies (list): Extract url for each movie and create a list after concatinating with base url..
        """
        content = PageFetcher.fetch_page(filmography_url)
        movies = []

        if content:
            soup = BeautifulSoup(content, "html.parser")
            table = soup.find(
                name="table", attrs={"class": "wikitable sortable plainrowheaders"}
            )
            if table:
                movie_links = table.find_all(name="a")
                movies = [
                    self.base_url + link["href"]
                    for link in movie_links
                    if link.get("href", "").startswith("/wiki/")
                ]

        return movies


class MovieDetailsExtractor:
    """
    This class is responsible for extracting details from each movie page.
    """

    @staticmethod
    def extract_movie_details(movie_url: str) -> dict:
        """This static methode responsible for extracting required movie information from each movie url from movies list

        Args:
            movie_url (str): movie url

        Returns:
           movie_details (dict): This will contains all required information for each movie.
        """
        movie_details = {}
        content = PageFetcher.fetch_page(movie_url)

        if content:
            soup = BeautifulSoup(content, "html.parser")
            main_table = soup.find(name="table", attrs={"class": "infobox vevent"})

            if main_table:
                main_body = main_table.find(name="tbody")
                for index, item in enumerate(main_body.find_all(name="tr")):
                    try:
                        if index == 0:  # First row contains the movie name
                            logger.info(
                                f"Extracting information for movie - {item.get_text(strip=True)}"
                            )
                            movie_details["movie_name"] = item.get_text(strip=True)
                        elif index > 1:  # Skipping rows that are not useful
                            if item.find(name="th") and item.find(name="td"):
                                movie_header = item.find(name="th").get_text(strip=True)
                                movie_header_data = [
                                    i.get_text(strip=True)
                                    for i in item.find_all(name="td")
                                ]
                                movie_details[movie_header] = movie_header_data
                        # logger.info(f'Movie details - {movie_details}')
                        # print(f'{movie_header} ---> {movie_header_data}')
                    except Exception as e:
                        logger.error(f"Error occurred for the movie {movie_url}: {e}")

        return movie_details


class MovieDetailsFetcher:
    """
    This class ties everything together to fetch movies and their details.
    """

    def __init__(self, base_url: str, filmography_url: str):
        """This will initilise the base and filmography url

        Args:
            base_url (str): Base url for each filmography url
            filmography_url (str): Filmography url contain all films
        """
        self.base_url = base_url
        self.filmography_url = filmography_url
        self.movies = []
        self.movies_detail_list = []

    def fetch_movie_details(self) -> list:
        """This methode is responsible for getting all movies details as list.

        Returns:
            moviles_detail_list (list) : This will contain list of all movies details dictonary
        """
        movie_parser = MovieParser(self.base_url)
        self.movies = movie_parser.parse_movies(self.filmography_url)

        for movie_url in self.movies:
            movie_details = MovieDetailsExtractor.extract_movie_details(movie_url)
            if movie_details:
                self.movies_detail_list.append(movie_details)

        return self.movies_detail_list
