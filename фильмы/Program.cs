using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace фильмы
{
    class Program
    {
        /*
        public static class PipelineStages 
        {
            public static Task ReadFilenamesAsync(string path, BlockingCollection<string> output)
            {
                return Task.Factory.StartNew(() =>
                {
                    foreach (string filename in Directory.EnumerateFiles(path, "*.cs", SearchOption.AllDirectories))
                    {
                        output.Add(filename); Console.WriteLine($"stage 1: added {filename}");
                    }
                    output.CompleteAdding();
                }, TaskCreationOptions.LongRunning);
            }

            public static async Task LoadContentAsync(BlockingCollection<string> input, BlockingCollection<string> output)
            { foreach (var filename in input.GetConsumingEnumerable())
                { using (FileStream stream = File.OpenRead(filename)) 
                    { var reader = new StreamReader(stream);
                        string line = null;
                        while ((line = await reader.ReadLineAsync()) != null) 
                        { output.Add(line); Console.WriteLine($"stage 2: added {line}");
                        } 
                    }
                } 
                output.CompleteAdding(); 
            }

            public static Task ProcessContentAsync(BlockingCollection<string> input, ConcurrentDictionary<string, int> output)
            {
                return Task.Factory.StartNew(() => {
                    foreach (var line in input.GetConsumingEnumerable())
                    {
                        string[] words = line.Split(' ', ';', '\t', '{', '}', '(', ')', ':', ',', '"'); foreach (var word in words.Where(w => !string.IsNullOrEmpty(w)))
                        {
                            output.AddOrUpdate(key: word, addValue: 1,
          updateValueFactory: (s, i) => ++i); Console.WriteLine($"stage 3: added {word}");
                        }
                    }
                }, TaskCreationOptions.LongRunning);
            }

            public static Task TransferContentAsync(ConcurrentDictionary<string, int> input, BlockingCollection<Info> output) 
            { 
                return Task.Factory.StartNew(() => 
                {
                    foreach (var word in input.Keys) 
                    { 
                        if (input.TryGetValue(word, out int value)) 
                        {
                            var info = new Info { Word = word, Count = value };
                            output.Add(info); 
                            Console.WriteLine($"stage 4: added {info}"); 
                        } 
                    } 
                    output.CompleteAdding(); 
                }, TaskCreationOptions.LongRunning); 
            }
        }
        */

        public static class PipeLineStages
        {
            public static ConcurrentDictionary<string, string> filmNames = new ConcurrentDictionary<string, string>();


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
        }

            public static Dictionary<string, string> MoviesID = IdFFilms();
        public static Dictionary<string, string> IdFFilms()//ключ-АЙДИ ФИЛЬМА, данные -НАЗВАНИЕ ФИЛЬМ 
        {
            Dictionary<string, string> films = new Dictionary<string, string>();//хотим вывести этот словарь (key - ID, value -фильм)
            
            using (StreamReader readtext = new StreamReader("C:\\Users\\Azat\\Desktop\\ml-latest\\MovieCodes_IMDB.tsv"))
            {
                string line = "";
                while ((line = readtext.ReadLine()) != null)
                {
                    string[] words = line.Split('	');
                    if (words[3] == "RU" || words[3] == "US"|| words[3]=="EN")
                    {
                        if (films.ContainsKey(words[0]))
                        {
                            films[words[0]] = words[2];
                        }
                        else
                        {
                            films.Add(words[0], words[2]);
                        }

                    }
                }
            }
            return films;
        }


        public static Dictionary<string, string> IdFFName()//ключ-АЙДИ ФИЛЬМА, данные-НАЗВАНИЕ ФИЛЬМА
        {
            Dictionary<string, string> dic = new Dictionary<string, string>();
            using (StreamReader readtext = new StreamReader("C:\\Users\\Azat\\Desktop\\ml-latest\\Ratings_IMDB.tsv"))
            {
                string line = readtext.ReadLine();
                while ((line = readtext.ReadLine()) != null)
                {
                    string[] words = line.Split('	');
                    if (dic.ContainsKey(words[0]))
                    {
                        dic[words[0]] = words[1];
                    }
                    else
                    {
                        dic[words[0]] = words[1];
                    }

                }
            }
            return dic;
        }

        public static Dictionary<string, string> IdFForTegIdFilms()//ключ-айди фильма для тега, данные - айди фльма 
        {
            Dictionary<string, string> dic = new Dictionary<string, string>();
            using (StreamReader readtext = new StreamReader("C:\\Users\\Azat\\Desktop\\ml-latest\\links_IMDB_MovieLens.csv"))
            {
                string line = "";
                while ((line = readtext.ReadLine()) != null)
                {
                    string[] words = line.Split(',');
                    if (dic.ContainsKey(words[0]))
                    {
                        dic[words[0]] = "tt" + words[1];
                    }
                    else
                    {
                        dic[words[0]] = "tt" + words[1];
                    }

                }
            }
            return dic;
        }

        public static Dictionary<string, string> GetTagsCodes()//id тега и сам тег
        {
            Dictionary<string, string> dic = new Dictionary<string, string>();
            using (StreamReader readtext = new StreamReader("C:\\Users\\Azat\\Desktop\\ml-latest\\TagCodes_MovieLens.csv"))
            {
                string line = "";
                while ((line = readtext.ReadLine()) != null)
                {
                    string[] words = line.Split(',');
                    if (dic.ContainsKey(words[0]))
                    {
                        dic[words[0]] = words[1];
                    }
                    else
                    {
                        dic[words[0]] = words[1];
                    }

                }
            }
            return dic;
        }
        public static Dictionary<string, List<string>> GetTagsScore() //сопоставляем айди фильма и тег
        {
            var FilmsId7TagId = IdFForTegIdFilms();
            var tag7Idtag = GetTagsCodes();
            Dictionary<string, List<string>> dic = new Dictionary<string, List<string>>();
            using (StreamReader readtext = new StreamReader("C:\\Users\\Azat\\Desktop\\ml-latest\\TagScores_MovieLens.csv"))
            {
                string line = "";
                while ((line = readtext.ReadLine()) != null)
                {
                    string[] words = line.Split(',');
                    if (MoviesID.ContainsKey(FilmsId7TagId[words[0]]) && float.Parse(words[2], CultureInfo.InvariantCulture.NumberFormat) > 0.5f)//существует ли фильм, которому хотим присвоить тег, а вторая на число >0.5
                    {
                        if (dic.ContainsKey(words[0]))
                        {
                            dic[FilmsId7TagId[words[0]]].Add(tag7Idtag[words[1]]);
                        }
                        else
                        {
                            if (FilmsId7TagId.ContainsKey(words[0]) && tag7Idtag.ContainsKey(words[1]))
                            {
                                dic[FilmsId7TagId[words[0]]] = new List<string>();
                                dic[FilmsId7TagId[words[0]]].Add(tag7Idtag[words[1]]);
                            }

                        }

                    }
                }
            }
            return dic;
        }

        public static Dictionary<string, string> GetAllActorsNames()//АЙДИ ЧЕЛОВЕКА И ЧЕЛОВЕК
        {
            Dictionary<string, string> dic = new Dictionary<string, string>();
           
            using (StreamReader readtext = new StreamReader("C:\\Users\\Azat\\Desktop\\ml-latest\\ActorsDirectorsNames_IMDB.txt"))
            {
                string line = "";
                while ((line = readtext.ReadLine()) != null)
                {
                    string[] words = line.Split('	');
                    if (dic.ContainsKey(words[0]))
                    {
                        dic[words[0]] = words[1];
                    }
                    else
                    {
                        dic[words[0]] = words[1];
                    }
                   
                }
            }
            
            return dic;
        }
        internal class Movie
        {
            public string Name { get; set; }
            public HashSet<string> Actors { get; set; }
            public HashSet<string> Tags { get; set; }
            public string Rating { get; set; }

            public Movie(string name, HashSet<string> actors, HashSet<string> tags, string rating)
            {
                Name = name;
                Actors = actors;
                Tags = tags;
                Rating = rating;
            }
        }
        public static Dictionary<string, List<string>> GetAllActorsCodes()//айди  филма - актеры(ид)
        {
            var ActorsNames = GetAllActorsNames();
            Dictionary<string, List<string>> dic = new Dictionary<string, List<string>>();
            using (StreamReader readtext = new StreamReader("C:\\Users\\Azat\\Desktop\\ml-latest\\ActorsDirectorsCodes_IMDB.tsv"))
            {
                string line = "";
                while ((line = readtext.ReadLine()) != null)
                {
                    string[] words = line.Split('	');

                    if (words[3] == "actor" || words[3] == "director") //
                    {
                        if (dic.ContainsKey(words[0]) && ActorsNames.ContainsKey(words[2]))
                        {
                            dic[words[0]].Add(ActorsNames[words[2]]);
                        }

                        else
                        {
                            if (ActorsNames.ContainsKey(words[2]))
                            {
                                dic[words[0]] = new List<string>();
                                dic[words[0]].Add(ActorsNames[words[2]]);
                            }
                        }


                        

                    }


                }
            }
            return dic;
        }

        public static List<Movie> GetMovies()
        {
            var Actors = GetAllActorsCodes();
            var tagsId = GetTagsScore();
            var Rating = IdFFName();

            List<Movie> movies = new List<Movie>();

            foreach (var key in MoviesID.Keys)
            {
                if (MoviesID.ContainsKey(key) && Actors.ContainsKey(key) && tagsId.ContainsKey(key) && Rating.ContainsKey(key))
                    movies.Add(new Movie(MoviesID[key], Actors[key].ToHashSet<string>(), tagsId[key].ToHashSet<string>(), Rating[key]));
            }

            return movies;
        }
        public static Dictionary<string, List<string>> ActorFilm()
        {
            Dictionary<string, List<string>> ActorFilms = GetAllActorsCodes();//ФИЛЬМ-Актеры
            Dictionary<string, List<string>> dic = new Dictionary<string, List<string>>();
            foreach (var key in ActorFilms.Keys)
            {
                if (!MoviesID.ContainsKey(key))
                {
                    continue;
                }
                foreach (var value in ActorFilms[key])
                {
                    if (dic.ContainsKey(value))
                    {
                        dic[value].Add(MoviesID[key]);
                    }
                    else
                    {
                        dic[value] = new List<string>();
                        dic[value].Add(MoviesID[key]);
                    }
                }
            }
            return dic;
        }
        public static Dictionary<string, List<string>> TagsFilms()//АЙДИ ТЕГА - ФИЛЬМ
        {
            Dictionary<string, List<string>> tagsId = GetTagsScore();//айди фильма и тег
            Dictionary<string, List<string>> dic = new Dictionary<string, List<string>>();
            foreach (var key in MoviesID.Keys)
            {
                if (!tagsId.ContainsKey(key))
                {
                    continue;
                }
                foreach (var tag in tagsId[key])
                {
                    if (dic.ContainsKey(tag))
                        dic[tag].Add(MoviesID[key]);
                    else
                    {
                        dic[tag] = new List<string>() {MoviesID[key]};
                    }

                }
            }
            return dic;
        }
        public static Dictionary<string, List<string>> TagsFilmsMOV()//АЙДИ ТЕГА - ФИЛЬМ
        {
            Dictionary<string, List<string>> tagsId = GetTagsScore();//айди фильма и тег
            Dictionary<string, List<string>> dic = new Dictionary<string, List<string>>();
            foreach (var key in MoviesID.Keys)
            {
                if (!tagsId.ContainsKey(key))
                {
                    continue;
                }
                foreach (var tag in tagsId[key])
                {
                    if (dic.ContainsKey(tag))
                        dic[tag].Add(MoviesID[key]);
                    else
                    {
                        dic[tag] = new List<string>() { MoviesID[key] };
                    }

                }
            }
            return dic;
        }

        static void Main(string[] args)
        {
              Stopwatch sw = new Stopwatch();
            
            sw.Start();
            var films = IdFFilms(); //4911
                sw.Stop();
               Console.WriteLine($"{sw.ElapsedMilliseconds} работает idfilms");
            Console.WriteLine(2);
            /*
             sw.Reset();
             sw.Start();
             var Actors1 = GetAllActorsCodes(); //68656
             sw.Stop();
             Console.WriteLine($"{sw.ElapsedMilliseconds} работает GetAllActorsCodes");

             sw.Reset();
             sw.Start();
             var tagsId1= GetTagsScore();//21704
             sw.Stop();
             Console.WriteLine($"{sw.ElapsedMilliseconds} работает GetTagsScore();");

             sw.Reset();
             sw.Start();
             var Rating1 = IdFFName();//1073
             sw.Stop();
             Console.WriteLine($"{sw.ElapsedMilliseconds} работает IdFFName();");

             PipeLineStages.ReadFileFilmsTitles("C:\\Users\\Azat\\Desktop\\ml-latest\\Ratings_IMDB.tsv");
            */
            


            sw.Reset();
            sw.Start();
            var t = PipeLineStages.ReadFileFilmsTitles("C:\\Users\\Azat\\Desktop\\ml-latest\\MovieCodes_IMDB.tsv");
            t.Wait();
            var aa = PipeLineStages.filmNames; // 5096
            sw.Stop();
            Console.WriteLine($"{sw.ElapsedMilliseconds} работает IdFFName() УЛУЧШЕННЫЙ;");

           // foreach ( var  key in aa.Keys.ToList())
          //  {
          //      Console.WriteLine(aa[key]);
          //  }

           // foreach (var key in films.Keys.ToList())
            //{
              //  Console.WriteLine(films[key]);
            //}

            var Movies = GetMovies();//1 





            Console.WriteLine(3);
            Dictionary<string, Movie> dic = new Dictionary<string, Movie>();


            Movies.ForEach(x => { if (dic.ContainsKey(x.Name)) { dic[x.Name] = x; } else { dic.Add(x.Name, x); } });//создал словарь из имени и значения(объект класса мувиес)

         
            foreach (var key in dic.Keys.ToList())
            {
                Console.WriteLine(key + "     " + String.Join(" , ",dic[key].Actors) + "   " + String.Join(" , ", dic[key].Tags) + "     " + dic[key].Rating);
            }
            var Actor = ActorFilm();
            Dictionary<string, Movie> dic2 = new Dictionary<string, Movie>();
        
            Movies.ForEach(x => { if (dic2.ContainsKey(x.Actors.ToString())) { dic2[x.Actors.ToString()] = x; } else { dic2.Add(x.Actors.ToString(), x); } });//создал словарь из имени и значения(объект класса мувиес)
            foreach (var key in dic2.Keys.ToList())
            {
                Console.WriteLine(key + "     " + String.Join(" , ", dic[key].Name) + "   " + String.Join(" , ", dic[key].Tags) + "     " + dic[key].Rating);
            }

            // Dictionary<string, Movie> dic3 = new Dictionary<string, Movie>();
            // Movies.ForEach(x => { if (dic3.ContainsKey(x.Actors.ToString())) { dic3[x.Actors.ToString()] = x; } else { dic3.Add(x.Actors.ToString(), x); } });
            //
         
        }
         

    }
}
