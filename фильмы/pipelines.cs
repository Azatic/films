using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace фильмы
{
}
    /*
    public class PipeLineStages
    {
        private static ConcurrentDictionary<string, string> filmNames = new();
        private static ConcurrentDictionary<string, string> filmRatings = new();
        private static Dictionary<string, string> personNames = new();
        private static Dictionary<string, HashSet<string>> directorsId = new();
        private static Dictionary<string, HashSet<string>> personIdFilmsId = new();
        private static Dictionary<string, HashSet<string>> actorsAndActreesId = new();
        private static ConcurrentDictionary<string, string> movieIdfilmId = new();
        private static ConcurrentDictionary<string, string> filmIdmovieId = new();
        private static Dictionary<string, HashSet<string>> tagsId = new(); //? movieId -> tagsId
        private static ConcurrentDictionary<string, string> tags = new(); //? tagId -> tagTitle

        // $ Это ответ 1
        public static Dictionary<string, Movie> movies = new();
        // $ Это ответ 2
        public static Dictionary<string, HashSet<Movie>> personMovies = new();
        // $ Это ответ 3
        public static Dictionary<string, HashSet<Movie>> tagMovies = new();
        public static Task ReadFileFilmsTitles(string path)
        {
            return Task.Run(() => {
                var lines = File.ReadAllLines(path).Skip(1);
                Parallel.ForEach(lines, line =>
                {
                    var lineSpan = line.AsSpan();
                    int index = lineSpan.IndexOf('\t');
                    var filmId = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);
                    index = lineSpan.IndexOf('\t');
                    lineSpan = lineSpan.Slice(index + 1);
                    index = lineSpan.IndexOf('\t');
                    var filmTitle = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);
                    index = lineSpan.IndexOf('\t');
                    var language = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);

                    if (language.Equals("RU") || language.Equals("US"))
                        filmNames.AddOrUpdate(key: filmId, addValue: filmTitle, updateValueFactory: (filmId, filmTitle) => filmTitle);
                });
            });
        }
        public static Task ReadFileFilmsRatings(string path)
        {
            return Task.Run(() => {
                var lines = File.ReadAllLines(path).Skip(1);
                Parallel.ForEach(lines, line =>
                {
                    var lineSpan = line.AsSpan();
                    int index = lineSpan.IndexOf('\t');
                    var filmId = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);
                    index = lineSpan.IndexOf('\t');
                    var rating = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);

                    filmRatings.AddOrUpdate(key: filmId, addValue: rating, updateValueFactory: (filmId, rating) => rating);
                });
            });
        }
        public static Task ReadFilePersonNames(string path)
        {
            return Task.Run(() => {
                var lines = File.ReadAllLines(path).Skip(1);
                Parallel.ForEach(lines, line =>
                {
                    var lineSpan = line.AsSpan();
                    int index = lineSpan.IndexOf('\t');
                    var personId = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);
                    index = lineSpan.IndexOf('\t');
                    var personName = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);
                    index = lineSpan.IndexOf('\t');
                    lineSpan = lineSpan.Slice(index + 1);
                    index = lineSpan.IndexOf('\t');
                    lineSpan = lineSpan.Slice(index + 1);
                    index = lineSpan.IndexOf('\t');
                    var category = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);
                    bool isHavingRole = false;

                    if ((category.Contains("acter") || category.Contains("director") || category.Contains("actrees")) && !personNames.ContainsKey(personId))
                    {
                        isHavingRole = true;
                        lock (personNames)
                            personNames.Add(personId, personName);
                    }
                    if (isHavingRole)
                    {
                        HashSet<string> films = new();
                        while (lineSpan.IndexOf(',') > 0)
                        {
                            index = lineSpan.IndexOf(',');
                            films.Add(lineSpan.Slice(0, index).ToString());
                            lineSpan = lineSpan.Slice(index + 1);
                        }
                        if (!personIdFilmsId.ContainsKey(personId))
                        {
                            lock (personIdFilmsId)
                            {
                                personIdFilmsId.Add(personId, films);
                            }
                        }
                    }
                });
            });

        }
        public static Task ReadFileTagsId(string path)
        {
            return Task.Run(() => {
                var lines = File.ReadAllLines(path).Skip(1);
                Parallel.ForEach(lines, line =>
                {
                    var lineSpan = line.AsSpan();
                    int index = lineSpan.IndexOf(',');
                    var movieId = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);
                    index = lineSpan.IndexOf(',');
                    var tagId = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);
                    var relevance = lineSpan.ToString();
                    relevance.Replace(',', '.');
                    if (relevance.Contains('.') && Double.Parse(relevance) > 0.5f)
                    {
                        lock (tagsId)
                        {
                            if (tagsId.ContainsKey(movieId))
                                tagsId[movieId].Add(tagId);
                            else
                                tagsId.Add(movieId, new HashSet<string> { tagId });
                        }
                    }
                });
            });
        }
        public static Task ReadFileTags(string path)
        {
            return Task.Run(() => {
                var lines = File.ReadAllLines(path).Skip(1);
                Parallel.ForEach(lines, line =>
                {
                    var lineSpan = line.AsSpan();
                    int index = lineSpan.IndexOf(',');
                    var tagId = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);
                    var tag = lineSpan.ToString();
                    tags.AddOrUpdate(key: tagId, addValue: tag, updateValueFactory: (tagId, tag) => tag);
                });
            });
        }
        public static Task ReadFileActorsDirectorsId(string path)
        {
            return Task.Run(() => {
                var lines = File.ReadAllLines(path).Skip(1);
                Parallel.ForEach(lines, line =>
                {
                    var lineSpan = line.AsSpan();
                    int index = lineSpan.IndexOf('\t');
                    var filmId = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);
                    index = lineSpan.IndexOf('\t');
                    lineSpan = lineSpan.Slice(index + 1);
                    index = lineSpan.IndexOf('\t');
                    var personId = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);
                    index = lineSpan.IndexOf('\t');
                    var category = lineSpan.Slice(0, index).ToString();

                    if (category.Equals("actor") || category.Equals("actress"))
                    {
                        lock (actorsAndActreesId)
                        {
                            if (actorsAndActreesId.ContainsKey(filmId))
                                actorsAndActreesId[filmId].Add(personId);
                            else
                                actorsAndActreesId.Add(filmId, new HashSet<string> { personId });
                        }
                    }
                    else if (category == "director")
                    {
                        lock (directorsId)
                        {
                            if (directorsId.ContainsKey(filmId))
                                directorsId[filmId].Add(personId);
                            else
                                directorsId.Add(filmId, new HashSet<string> { personId });
                        }
                    }
                });
            });
        }
        public static Task ReadFileFilmMovieId(string path)
        {
            return Task.Run(() => {
                var lines = File.ReadAllLines(path).Skip(1);
                Parallel.ForEach(lines, line => {
                    var lineSpan = line.AsSpan();
                    int index = lineSpan.IndexOf(',');
                    var movieId = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);
                    index = lineSpan.IndexOf(',');
                    var filmId = lineSpan.Slice(0, index).ToString();
                    while (filmId.Length != 7)
                        filmId = "0" + filmId;
                    filmId = "tt" + filmId;
                    filmIdmovieId.AddOrUpdate(key: filmId, addValue: movieId, updateValueFactory: (filmId, movieId) => movieId);
                    movieIdfilmId.AddOrUpdate(key: movieId, addValue: filmId, updateValueFactory: (movieId, filmId) => filmId);
                });
            });
        }
        public static Task CreateMovieObjects()
        {
            return Task.Run(() => {
                Parallel.ForEach(filmNames.Keys, filmId => {
                    lock (movies)
                    {
                        if (!movies.ContainsKey(filmNames[filmId]))
                        {
                            Movie movie = new();
                            movie.filmName = filmNames[filmId];
                            if (filmRatings.ContainsKey(filmId))
                                movie.rating = filmRatings[filmId];

                            var task1 = AddActors(movie, filmId);
                            var task2 = AddDirectors(movie, filmId);
                            var task3 = AddTags(movie, filmId);
                            Task.WaitAll(task1, task2, task3);

                            movies.Add(movie.filmName, movie);
                        }
                    }
                });
            });
        }
        private static Task AddActors(Movie movie, string filmId)
        {
            return Task.Run(() => {
                if (actorsAndActreesId.ContainsKey(filmId))
                    foreach (var actorId in actorsAndActreesId[filmId])
                    {
                        if (personNames.ContainsKey(actorId))
                            movie.actors.Add(personNames[actorId]);
                    }

            });
        }
        private static Task AddDirectors(Movie movie, string filmId)
        {
            return Task.Run(() => {
                if (directorsId.ContainsKey(filmId))
                    foreach (var directorId in directorsId[filmId])
                    {
                        if (personNames.ContainsKey(directorId))
                            movie.actors.Add(personNames[directorId]);
                    }
            });
        }
        private static Task AddTags(Movie movie, string filmId)
        {
            return Task.Run(() => {
                if (filmIdmovieId.ContainsKey(filmId) && tagsId.ContainsKey(filmIdmovieId[filmId]))
                    foreach (var tagId in tagsId[filmIdmovieId[filmId]])
                        movie.tags.Add(tags[tagId]);
            });
        }
        public static Task CreatePersonFilms()
        {
            return Task.Run(() => {
                Parallel.ForEach(personIdFilmsId, pair =>
                {
                    foreach (var filmId in pair.Value)
                    {
                        lock (personMovies)
                        {
                            if ((actorsAndActreesId.ContainsKey(filmId) || directorsId.ContainsKey(filmId))
                                                                        && personNames.ContainsKey(pair.Key)
                                                                        && filmNames.ContainsKey(filmId)
                                                                        && movies.ContainsKey(filmNames[filmId]))
                            {
                                if (personMovies.ContainsKey(personNames[pair.Key]))
                                    personMovies[personNames[pair.Key]].Add(movies[filmNames[filmId]]);
                                else if (filmNames.ContainsKey(filmId))
                                    personMovies.Add(personNames[pair.Key], new HashSet<Movie> { movies[filmNames[filmId]] });
                            }
                        }
                    }
                });
            });
        }
        public static Task CreateTagFilms()
        {
            return Task.Run(() => {
                Parallel.ForEach(tags, pair =>
                {
                    string tagId = pair.Key;
                    string tagTitle = pair.Value;
                    foreach (var movieIdTagsId in tagsId)
                    {
                        if (movieIdTagsId.Value.Contains(tagId)
                        && movieIdfilmId.ContainsKey(movieIdTagsId.Key)
                        && filmNames.ContainsKey(movieIdfilmId[movieIdTagsId.Key])
                        && movies.ContainsKey(filmNames[movieIdfilmId[movieIdTagsId.Key]]))
                        {
                            lock (tagMovies)
                            {
                                if (tagMovies.ContainsKey(tagTitle))
                                    tagMovies[tagTitle].Add(movies[filmNames[movieIdfilmId[movieIdTagsId.Key]]]);
                                else
                                    tagMovies.Add(tagTitle, new HashSet<Movie> { movies[filmNames[movieIdfilmId[movieIdTagsId.Key]]] });
                            }

                        }
                    }
                });
            });


        }
    }

}
    */