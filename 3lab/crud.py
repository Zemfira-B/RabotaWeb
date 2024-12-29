from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import asyncpg
from contextlib import asynccontextmanager
from typing import List

class MovieBase(BaseModel):
    name: str
    description: str
    popularity: int

class MovieCreate(MovieBase):
    pass

class Movie(MovieBase):
    id: int

class GenreBase(BaseModel):
    name: str
    description: str
    popularity: int

class GenreCreate(GenreBase):
    pass

class Genre(GenreBase):
    id: int

DATABASE_URL = "postgresql://postgres:ZemfiraB@localhost:5432/movies_db"

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL)
    yield
    await app.state.pool.close()

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def read_root():
    return {"message": "Welcome to the Movies API!"}

@app.get("/movies/", response_model=List[Movie])
async def read_movies():
    async with app.state.pool.acquire() as connection:
        movies = await connection.fetch("SELECT * FROM movies")
        return [Movie(id=movie["id"], name=movie["name"], description=movie["description"], popularity=movie["popularity"]) for movie in movies]

@app.get("/movies/{id}", response_model=Movie)
async def read_movie(id: int):
    async with app.state.pool.acquire() as connection:
        movie = await connection.fetchrow("SELECT * FROM movies WHERE id = $1", id)
        if movie:
            return Movie(id=movie["id"], name=movie["name"], description=movie["description"], popularity=movie["popularity"])
        raise HTTPException(status_code=404, detail="Movie not found")

@app.post("/movies/", response_model=Movie)
async def create_movie(movie: MovieCreate):
    async with app.state.pool.acquire() as connection:
        new_movie_id = await connection.fetchval("INSERT INTO movies (name, description, popularity) VALUES ($1, $2, $3) RETURNING id", movie.name, movie.description, movie.popularity)
        return Movie(id=new_movie_id, **movie.dict())

@app.put("/movies/{movie_id}", response_model=Movie)
async def update_movie(movie_id: int, updated_movie: MovieCreate):
    async with app.state.pool.acquire() as connection:
        result = await connection.execute("UPDATE movies SET name = $1, description = $2, popularity = $3 WHERE id = $4", updated_movie.name, updated_movie.description, updated_movie.popularity, movie_id)
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Movie not found")
        return Movie(id=movie_id, **updated_movie.dict())

@app.delete("/movies/{movie_id}")
async def delete_movie(movie_id: int):
    async with app.state.pool.acquire() as connection:
        result = await connection.execute("DELETE FROM movies WHERE id = $1", movie_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Movie not found")
        return {"message": "Movie deleted"}

@app.get("/genres/", response_model=List[Genre])
async def read_genres():
    async with app.state.pool.acquire() as connection:
        genres = await connection.fetch("SELECT * FROM genres")
        return [Genre(id=genre["id"], name=genre["name"], description=genre["description"], popularity=genre["popularity"]) for genre in genres]

@app.get("/genres/{id}", response_model=Genre)
async def read_genre(id: int):
    async with app.state.pool.acquire() as connection:
        genre = await connection.fetchrow("SELECT * FROM genres WHERE id = $1", id)
        if genre:
            return Genre(id=genre["id"], name=genre["name"], description=genre["description"], popularity=genre["popularity"])
        raise HTTPException(status_code=404, detail="Genre not found")

@app.post("/genres/", response_model=Genre)
async def create_genre(genre: GenreCreate):
    async with app.state.pool.acquire() as connection:
        new_genre_id = await connection.fetchval("INSERT INTO genres (name, description, popularity) VALUES ($1, $2, $3) RETURNING id", genre.name, genre.description, genre.popularity)
        return Genre(id=new_genre_id, **genre.dict())

@app.put("/genres/{genre_id}", response_model=Genre)
async def update_genre(genre_id: int, updated_genre: GenreCreate):
    async with app.state.pool.acquire() as connection:
        result = await connection.execute("UPDATE genres SET name = $1, description = $2, popularity = $3 WHERE id = $4", updated_genre.name, updated_genre.description, updated_genre.popularity, genre_id)
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Genre not found")
        return Genre(id=genre_id, **updated_genre.dict())

@app.delete("/genres/{genre_id}")
async def delete_genre(genre_id: int):
    async with app.state.pool.acquire() as connection:
        result = await connection.execute("DELETE FROM genres WHERE id = $1", genre_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Genre not found")
        return {"message": "Genre deleted"}

if __name__ == "__main__":
    uvicorn.run(app, host='127.0.0.1', port=8002)
