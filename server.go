package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"golang.org/x/net/http2"
)

var dbConnect *sql.DB

func main() {
	fmt.Println("Starting Sample Rest Server...")

	// Open connection to ClickHouse database
	var err error
	dbConnect, err = sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")

	// Log error if is not nil
	if err != nil {
		log.Fatal(err)
	}

	// Ping to check db connection
	if err = dbConnect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	// Create new ECHO instance
	e := echo.New()

	// Use middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Create API routes
	e.POST("/create/table", createTable)
	e.POST("/create/user", createNewUser)
	e.POST("/delete/user/:id", deleteUserById)

	e.GET("/users", getAllUsers)
	e.GET("/users/:id", getUserById)
	e.GET("/", hello)

	// Create HTTP/2 server
	s := &http2.Server{
		MaxConcurrentStreams: 250,
		MaxReadFrameSize:     1048576,
		IdleTimeout:          10 * time.Second,
	}

	// Start server
	e.Logger.Fatal(e.StartH2CServer(":8000", s))
}

// General handler
type User struct {
	Name string `json:"name" xml:"name"`
	Age  int    `json:"age" xml:"age"`
}

func hello(c echo.Context) error {
	u := &User{
		Name: "Vincent",
		Age:  28,
	}

	return c.JSON(http.StatusOK, u)
}

// Create new table handler
func createTable(c echo.Context) error {
	_, err := dbConnect.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id UInt8 NOT NULL,
			full_name String,
			account String,
			dob Date,
			phone_number String
		) ENGINE=MergeTree()
		ORDER BY (id)
		PRIMARY KEY (id)
	`)

	if err != nil {
		log.Fatal(err)
	}

	return c.String(http.StatusOK, err.Error())
}

// Get all users handler
func getAllUsers(c echo.Context) error {
	rows, err := dbConnect.Query("SELECT * FROM users")

	for rows.Next() {
		var (
			id          uint8
			fullName    string
			account     string
			dob         string
			phoneNumber string
		)
		if err := rows.Scan(&id, &fullName, &account, &dob, &phoneNumber); err != nil {
			log.Fatal(err)
		}
		log.Printf("id: %d, full_name: %s, account: %s, phone_number: %s", id, fullName, account, phoneNumber)
	}

	// rows, err := dbConnect.Exec(`
	// 	SELECT * FROM users
	// `)

	if err != nil {
		log.Fatal(err)
	}

	return c.JSON(http.StatusOK, rows)
}

// Create new user handler
func createNewUser(c echo.Context) error {
	id, err := strconv.ParseUint(c.FormValue("id"), 10, 8)
	if err != nil {
		log.Fatal(err)
	}
	fullName := c.FormValue("full_name")
	account := c.FormValue("account")
	phoneNumber := c.FormValue("phone_number")

	log.Printf("Create new user: id: %d, full_name: %s, account: %s, phone_number: %s", id, fullName, account, phoneNumber)

	tx, _ := dbConnect.Begin()

	stmt, _ := tx.Prepare("INSERT INTO users (id, full_name, account, phone_number) VALUES (?, ?, ?, ?)")
	defer stmt.Close()

	if _, err := stmt.Exec(
		id, fullName,
		account, phoneNumber,
	); err != nil {
		log.Fatal(err)
	}

	if err = tx.Commit(); err != nil {
		log.Fatal(err)
	}

	return c.String(http.StatusOK, "Created new User")
}

// Delete user by id handler
func deleteUserById(c echo.Context) error {
	id := c.Param("id")
	return c.String(http.StatusOK, "Deleted user by id = "+id)
}

// Get user by id handler
func getUserById(c echo.Context) error {
	id := c.Param("id")
	return c.String(http.StatusOK, "User by id ="+id)
}
