# OC Operator Tools

OC Operator Tools is a web app built by an operator, for operators. It brings together block lookups, paddle viewing, shuttle schedules, booking-board references, and live bus location checks in one place so operators can quickly find the information they need during sign-in, reliefs, planning, and day-to-day work.

Live app: [https://oc-bus-tracker.vercel.app/](https://oc-bus-tracker.vercel.app/)

## What it does

- Look up a block and get live bus location information when available
- Show available paddle versions for a block by service day and booking period
- Open full paddles with trips, notes, relief points, and deadhead directions
- Open Google Maps directions for deadhead segments
- Look up a bus number directly and show current location information when available
- Show shuttle schedules for weekday, Saturday, and Sunday service
- Highlight the active trip or next stop when a shuttle or paddle is live for the current day
- Save work blocks and shuttles with an account for quick lookup
- Review booking-board information and open linked paddle details

## Main workflows

### 1. Home screen

Use the main search experience to look up blocks, buses, paddles, and shuttle information from one place.

![Home screen](assets/screenshots/home-current.png)

### 2. Block lookup

Enter a block number to get the currently assigned live bus when available. If paddle data exists, the app also shows paddle options below the reply.

![Block lookup example](assets/screenshots/block-lookup-current.png)

### 3. Paddle viewer

Open the full paddle for the selected service day. The paddle viewer includes trips, start details, relief details, end details, notes, deadhead directions, and map links.

![Paddle viewer](assets/screenshots/paddle-view-current.png)

### 4. Shuttle lookup

Open shuttle schedules and switch between weekday, Saturday, and Sunday service.

![Shuttle list](assets/screenshots/shuttle-list-current.png)

### 5. Shuttle schedule

Open any shuttle to view its full schedule. When that shuttle is live for the current day, the app shows the next stop summary and highlights the relevant trip.

![Shuttle schedule](assets/screenshots/shuttle-schedule-current.png)

## Notes

- Live bus data depends on upstream availability and may occasionally be missing.
- Paddle, shuttle, and booking-board information is intended as an operator reference.
- Operators should always follow official paddle and deadhead instructions.
- Do not use your phone while operating a bus.
