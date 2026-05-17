"""Anthropic tool definitions for all Orby skills."""

TOOLS = [
    # ── REMINDERS ──────────────────────────────────────────────────────────
    {
        "name": "set_reminder",
        "description": "Set a reminder for the user. Use when they say 'remind me', 'don't let me forget', 'set a reminder'.",
        "input_schema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "What to remind them about"},
                "when": {"type": "string", "description": "ISO 8601 datetime, e.g. 2026-05-16T15:00:00"},
                "recurring": {"type": "string", "description": "daily, weekly, monthly, or null"}
            },
            "required": ["text", "when"]
        }
    },
    {
        "name": "get_reminders",
        "description": "Get upcoming reminders. Use when user asks what reminders they have.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "complete_reminder",
        "description": "Mark a reminder as done.",
        "input_schema": {
            "type": "object",
            "properties": {"reminder_id": {"type": "string"}},
            "required": ["reminder_id"]
        }
    },

    # ── CALENDAR ───────────────────────────────────────────────────────────
    {
        "name": "add_calendar_event",
        "description": "Add an event to the personal or family calendar. Use when user mentions scheduling something.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "start": {"type": "string", "description": "ISO 8601 datetime"},
                "end": {"type": "string", "description": "ISO 8601 datetime (optional)"},
                "notes": {"type": "string"},
                "family": {"type": "boolean", "description": "true = family calendar, false = personal (default)"}
            },
            "required": ["title", "start"]
        }
    },
    {
        "name": "get_calendar",
        "description": "Get calendar events for today or a date range.",
        "input_schema": {
            "type": "object",
            "properties": {
                "family": {"type": "boolean"},
                "date": {"type": "string", "description": "YYYY-MM-DD, defaults to today"}
            }
        }
    },

    # ── TO-DO ──────────────────────────────────────────────────────────────
    {
        "name": "add_todo",
        "description": "Add a task to the personal or family to-do list.",
        "input_schema": {
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "family": {"type": "boolean", "description": "true = family list"},
                "priority": {"type": "string", "enum": ["high", "normal", "low"]}
            },
            "required": ["text"]
        }
    },
    {
        "name": "get_todos",
        "description": "Get open to-do items.",
        "input_schema": {
            "type": "object",
            "properties": {"family": {"type": "boolean"}}
        }
    },
    {
        "name": "complete_todo",
        "description": "Mark a to-do item as done.",
        "input_schema": {
            "type": "object",
            "properties": {
                "item_id": {"type": "string"},
                "family": {"type": "boolean"}
            },
            "required": ["item_id"]
        }
    },

    # ── NOTES ──────────────────────────────────────────────────────────────
    {
        "name": "save_note",
        "description": "Save a note or piece of information the user wants to remember.",
        "input_schema": {
            "type": "object",
            "properties": {
                "content": {"type": "string"},
                "title": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["content"]
        }
    },
    {
        "name": "search_notes",
        "description": "Search saved notes.",
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }
    },

    # ── SHOPPING ───────────────────────────────────────────────────────────
    {
        "name": "add_shopping_item",
        "description": "Add an item to the shopping list.",
        "input_schema": {
            "type": "object",
            "properties": {
                "item": {"type": "string"},
                "quantity": {"type": "string"},
                "family": {"type": "boolean"}
            },
            "required": ["item"]
        }
    },
    {
        "name": "get_shopping_list",
        "description": "Get the current shopping list.",
        "input_schema": {
            "type": "object",
            "properties": {"family": {"type": "boolean"}}
        }
    },

    # ── JOURNAL ────────────────────────────────────────────────────────────
    {
        "name": "add_journal_entry",
        "description": "Write a journal or diary entry, or a dream journal entry.",
        "input_schema": {
            "type": "object",
            "properties": {
                "content": {"type": "string"},
                "mood": {"type": "string"},
                "is_dream": {"type": "boolean"}
            },
            "required": ["content"]
        }
    },
    {
        "name": "get_journal_entries",
        "description": "Read recent journal entries.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer"},
                "is_dream": {"type": "boolean"}
            }
        }
    },

    # ── MOOD ───────────────────────────────────────────────────────────────
    {
        "name": "log_mood",
        "description": "Log the user's current mood.",
        "input_schema": {
            "type": "object",
            "properties": {
                "mood": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["mood"]
        }
    },

    # ── WEATHER ────────────────────────────────────────────────────────────
    {
        "name": "get_weather",
        "description": "Get current weather and forecast for a location.",
        "input_schema": {
            "type": "object",
            "properties": {"location": {"type": "string", "description": "City name or address"}},
            "required": ["location"]
        }
    },

    # ── FINANCE ────────────────────────────────────────────────────────────
    {
        "name": "log_expense",
        "description": "Log a purchase or expense the user mentions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "amount": {"type": "number"},
                "description": {"type": "string"},
                "store": {"type": "string"},
                "payment_method": {"type": "string", "description": "e.g. Visa, checking, cash, Amex"},
                "category": {"type": "string"}
            },
            "required": ["amount", "description"]
        }
    },
    {
        "name": "log_income",
        "description": "Log income or a paycheck.",
        "input_schema": {
            "type": "object",
            "properties": {
                "amount": {"type": "number"},
                "source": {"type": "string"}
            },
            "required": ["amount"]
        }
    },
    {
        "name": "add_account",
        "description": "Add or update a bank account or credit card for balance tracking.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "e.g. Chase Checking, Visa, Amex"},
                "account_type": {"type": "string", "enum": ["checking", "savings", "credit", "debit", "cash"]},
                "balance": {"type": "number"},
                "limit": {"type": "number", "description": "Credit limit (for credit cards only)"}
            },
            "required": ["name", "account_type", "balance"]
        }
    },
    {
        "name": "get_finance_summary",
        "description": "Get financial summary — balances, spending, income for the month.",
        "input_schema": {"type": "object", "properties": {}}
    },

    # ── FAMILY MESSAGING ───────────────────────────────────────────────────
    {
        "name": "send_family_message",
        "description": "Send a message to a family member or the whole family through their Orby.",
        "input_schema": {
            "type": "object",
            "properties": {
                "recipient": {"type": "string", "description": "Person's name or 'everyone'"},
                "content": {"type": "string"},
                "urgent": {"type": "boolean"}
            },
            "required": ["recipient", "content"]
        }
    },

    # ── RECIPES ────────────────────────────────────────────────────────────
    {
        "name": "save_recipe",
        "description": "Save a recipe.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "ingredients": {"type": "array", "items": {"type": "string"}},
                "steps": {"type": "array", "items": {"type": "string"}},
                "tags": {"type": "array", "items": {"type": "string"}},
                "servings": {"type": "string"},
                "source_url": {"type": "string"}
            },
            "required": ["name", "ingredients", "steps"]
        }
    },
    {
        "name": "find_recipe",
        "description": "Search saved recipes by name or ingredient.",
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }
    },

    # ── HABITS ─────────────────────────────────────────────────────────────
    {
        "name": "add_habit",
        "description": "Create a new habit to track.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "frequency": {"type": "string", "enum": ["daily", "weekly", "monthly"]},
                "goal": {"type": "string"}
            },
            "required": ["name"]
        }
    },
    {
        "name": "log_habit",
        "description": "Log that a habit was done (or not done) today.",
        "input_schema": {
            "type": "object",
            "properties": {
                "habit_name": {"type": "string"},
                "done": {"type": "boolean"}
            },
            "required": ["habit_name"]
        }
    },

    # ── FITNESS ────────────────────────────────────────────────────────────
    {
        "name": "log_workout",
        "description": "Log a workout or exercise session.",
        "input_schema": {
            "type": "object",
            "properties": {
                "exercise": {"type": "string"},
                "duration_minutes": {"type": "integer"},
                "sets": {"type": "integer"},
                "reps": {"type": "integer"},
                "weight": {"type": "number"},
                "notes": {"type": "string"}
            },
            "required": ["exercise"]
        }
    },

    # ── HEALTH ─────────────────────────────────────────────────────────────
    {
        "name": "log_health",
        "description": "Log health metrics — weight, sleep, water, symptoms, medications.",
        "input_schema": {
            "type": "object",
            "properties": {
                "weight": {"type": "number"},
                "sleep_hours": {"type": "number"},
                "water_glasses": {"type": "integer"},
                "symptoms": {"type": "array", "items": {"type": "string"}},
                "medications": {"type": "array", "items": {"type": "string"}},
                "notes": {"type": "string"}
            }
        }
    },
    {
        "name": "add_medication_reminder",
        "description": "Add a daily medication reminder.",
        "input_schema": {
            "type": "object",
            "properties": {
                "med_name": {"type": "string"},
                "time_of_day": {"type": "string"},
                "dose": {"type": "string"}
            },
            "required": ["med_name", "time_of_day"]
        }
    },

    # ── PEOPLE / RELATIONSHIPS ─────────────────────────────────────────────
    {
        "name": "save_person",
        "description": "Save information about someone in the user's life — family, friends, etc.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "relation": {"type": "string", "description": "e.g. mom, best friend, coworker"},
                "birthday": {"type": "string", "description": "MM-DD or YYYY-MM-DD"},
                "anniversary": {"type": "string"},
                "phone": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["name"]
        }
    },

    # ── GIFTS ──────────────────────────────────────────────────────────────
    {
        "name": "add_gift_idea",
        "description": "Save a gift idea for someone.",
        "input_schema": {
            "type": "object",
            "properties": {
                "person": {"type": "string"},
                "idea": {"type": "string"},
                "occasion": {"type": "string"},
                "budget": {"type": "string"}
            },
            "required": ["person", "idea"]
        }
    },

    # ── PETS ───────────────────────────────────────────────────────────────
    {
        "name": "log_pet_care",
        "description": "Log a vet visit, medication, or note for a pet.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pet_name": {"type": "string"},
                "care_type": {"type": "string", "enum": ["vet_visit", "medication", "note", "feeding"]},
                "notes": {"type": "string"},
                "date": {"type": "string"}
            },
            "required": ["pet_name", "care_type"]
        }
    },

    # ── VEHICLES ───────────────────────────────────────────────────────────
    {
        "name": "log_vehicle_service",
        "description": "Log vehicle maintenance or service.",
        "input_schema": {
            "type": "object",
            "properties": {
                "vehicle": {"type": "string"},
                "service_type": {"type": "string"},
                "mileage": {"type": "integer"},
                "cost": {"type": "number"},
                "notes": {"type": "string"}
            },
            "required": ["vehicle", "service_type"]
        }
    },

    # ── TRAVEL ─────────────────────────────────────────────────────────────
    {
        "name": "add_trip",
        "description": "Save a trip or vacation plan.",
        "input_schema": {
            "type": "object",
            "properties": {
                "destination": {"type": "string"},
                "start_date": {"type": "string"},
                "end_date": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["destination"]
        }
    },

    # ── COUNTDOWN ──────────────────────────────────────────────────────────
    {
        "name": "add_countdown",
        "description": "Create a countdown to an event or date.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "e.g. Christmas, My Vacation, Wedding"},
                "target_date": {"type": "string", "description": "YYYY-MM-DD"}
            },
            "required": ["name", "target_date"]
        }
    },

    # ── BUCKET LIST ────────────────────────────────────────────────────────
    {
        "name": "add_bucket_list_item",
        "description": "Add something to the user's bucket list.",
        "input_schema": {
            "type": "object",
            "properties": {
                "item": {"type": "string"},
                "category": {"type": "string", "description": "e.g. travel, adventure, career, family"},
                "notes": {"type": "string"}
            },
            "required": ["item"]
        }
    },
    {
        "name": "get_bucket_list",
        "description": "Get the user's bucket list.",
        "input_schema": {"type": "object", "properties": {}}
    },

    # ── QUOTES ─────────────────────────────────────────────────────────────
    {
        "name": "save_quote",
        "description": "Save an inspiring quote or saying the user wants to keep.",
        "input_schema": {
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "author": {"type": "string"}
            },
            "required": ["text"]
        }
    },

    # ── MORNING BRIEFING ───────────────────────────────────────────────────
    {
        "name": "get_morning_briefing",
        "description": "Get the full daily morning briefing — schedule, reminders, weather, family messages, and more.",
        "input_schema": {"type": "object", "properties": {}}
    },

    # ── EMERGENCY CONTACT ──────────────────────────────────────────────────
    {
        "name": "set_emergency_contact",
        "description": "Set an emergency contact for the user's wellbeing system.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "relation": {"type": "string"}
            },
            "required": ["name"]
        }
    },

    # ── MEAL PLANNING ──────────────────────────────────────────────────────
    {
        "name": "plan_meal",
        "description": "Plan a meal for a specific day. Use when someone says what they're having for dinner, planning the week's meals, or scheduling any meal.",
        "input_schema": {
            "type": "object",
            "properties": {
                "day": {"type": "string", "description": "YYYY-MM-DD"},
                "meal_type": {"type": "string", "enum": ["breakfast", "lunch", "dinner", "snack"]},
                "dish": {"type": "string"},
                "servings": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["day", "meal_type", "dish"]
        }
    },
    {
        "name": "get_meal_plan",
        "description": "Get the meal plan for today or the week.",
        "input_schema": {
            "type": "object",
            "properties": {
                "day": {"type": "string", "description": "YYYY-MM-DD for a specific day, or omit for today"},
                "week": {"type": "boolean", "description": "true to get the full week"}
            }
        }
    },

    # ── CHORES ─────────────────────────────────────────────────────────────
    {
        "name": "add_chore",
        "description": "Add a household chore to the chore chart. Assign it to a family member and set points.",
        "input_schema": {
            "type": "object",
            "properties": {
                "chore": {"type": "string"},
                "assigned_to": {"type": "string", "description": "Family member's name"},
                "frequency": {"type": "string", "enum": ["daily", "weekly", "monthly", "as-needed"]},
                "points": {"type": "integer", "description": "Points earned for completing this chore"}
            },
            "required": ["chore"]
        }
    },
    {
        "name": "complete_chore",
        "description": "Mark a chore as done for today.",
        "input_schema": {
            "type": "object",
            "properties": {
                "chore_id": {"type": "string"},
                "completed_by": {"type": "string"}
            },
            "required": ["chore_id"]
        }
    },
    {
        "name": "get_chores",
        "description": "Get the household chore list.",
        "input_schema": {
            "type": "object",
            "properties": {
                "assigned_to": {"type": "string", "description": "Filter by family member name"}
            }
        }
    },
    {
        "name": "get_chore_points",
        "description": "Get points/leaderboard for chore completion.",
        "input_schema": {
            "type": "object",
            "properties": {
                "person": {"type": "string", "description": "Specific person, or omit for full leaderboard"}
            }
        }
    },

    # ── HOME MAINTENANCE ───────────────────────────────────────────────────
    {
        "name": "log_home_repair",
        "description": "Log a home repair or maintenance task.",
        "input_schema": {
            "type": "object",
            "properties": {
                "item": {"type": "string", "description": "What was repaired, e.g. 'kitchen faucet', 'HVAC'"},
                "description": {"type": "string"},
                "cost": {"type": "number"},
                "contractor": {"type": "string"},
                "date": {"type": "string", "description": "YYYY-MM-DD, defaults to today"}
            },
            "required": ["item", "description"]
        }
    },
    {
        "name": "add_warranty",
        "description": "Save a warranty for an appliance or item.",
        "input_schema": {
            "type": "object",
            "properties": {
                "item": {"type": "string"},
                "expires": {"type": "string", "description": "YYYY-MM-DD expiration date"},
                "purchase_date": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["item", "expires"]
        }
    },
    {
        "name": "save_contractor",
        "description": "Save a contractor's contact info (plumber, electrician, etc.).",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "trade": {"type": "string", "description": "e.g. plumber, electrician, HVAC, painter"},
                "phone": {"type": "string"},
                "email": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["name", "trade"]
        }
    },
    {
        "name": "find_contractor",
        "description": "Find a saved contractor by trade.",
        "input_schema": {
            "type": "object",
            "properties": {
                "trade": {"type": "string", "description": "e.g. plumber, electrician"}
            },
            "required": ["trade"]
        }
    },

    # ── EMERGENCY INFO ─────────────────────────────────────────────────────
    {
        "name": "save_emergency_contact",
        "description": "Save an emergency contact for the family.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "relation": {"type": "string", "description": "e.g. neighbor, doctor, family friend"},
                "phone": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["name", "relation", "phone"]
        }
    },
    {
        "name": "save_medical_info",
        "description": "Save medical info for a family member — allergies, blood type, conditions, doctor info.",
        "input_schema": {
            "type": "object",
            "properties": {
                "person": {"type": "string"},
                "info_type": {"type": "string", "description": "allergy, blood_type, condition, doctor, medication, hospital"},
                "details": {"type": "string"}
            },
            "required": ["person", "info_type", "details"]
        }
    },
    {
        "name": "save_insurance",
        "description": "Save insurance policy information.",
        "input_schema": {
            "type": "object",
            "properties": {
                "insurance_type": {"type": "string", "enum": ["health", "auto", "home", "life", "dental", "vision", "other"]},
                "provider": {"type": "string"},
                "policy_number": {"type": "string"},
                "phone": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["insurance_type", "provider", "policy_number"]
        }
    },
    {
        "name": "get_emergency_info",
        "description": "Get emergency contacts, medical info, or insurance information.",
        "input_schema": {
            "type": "object",
            "properties": {
                "info_type": {"type": "string", "enum": ["contacts", "medical", "insurance", "all"]},
                "person": {"type": "string", "description": "Filter medical info by family member"}
            }
        }
    },

    # ── SCHOOL / HOMEWORK ──────────────────────────────────────────────────
    {
        "name": "add_homework",
        "description": "Add a homework assignment for a child.",
        "input_schema": {
            "type": "object",
            "properties": {
                "child_name": {"type": "string"},
                "subject": {"type": "string"},
                "description": {"type": "string"},
                "due_date": {"type": "string", "description": "YYYY-MM-DD"}
            },
            "required": ["child_name", "subject", "description", "due_date"]
        }
    },
    {
        "name": "complete_homework",
        "description": "Mark homework as done.",
        "input_schema": {
            "type": "object",
            "properties": {"homework_id": {"type": "string"}},
            "required": ["homework_id"]
        }
    },
    {
        "name": "get_homework",
        "description": "Get open homework assignments.",
        "input_schema": {
            "type": "object",
            "properties": {
                "child_name": {"type": "string", "description": "Filter by child's name"}
            }
        }
    },
    {
        "name": "save_teacher",
        "description": "Save a teacher's contact information.",
        "input_schema": {
            "type": "object",
            "properties": {
                "child_name": {"type": "string"},
                "teacher_name": {"type": "string"},
                "subject": {"type": "string"},
                "phone": {"type": "string"},
                "email": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["child_name", "teacher_name", "subject"]
        }
    },
    {
        "name": "log_grade",
        "description": "Log a grade or test score for a child.",
        "input_schema": {
            "type": "object",
            "properties": {
                "child_name": {"type": "string"},
                "subject": {"type": "string"},
                "grade": {"type": "string", "description": "e.g. A, 92%, B+"},
                "assignment": {"type": "string"},
                "date": {"type": "string"}
            },
            "required": ["child_name", "subject", "grade"]
        }
    },

    # ── ALLOWANCE ──────────────────────────────────────────────────────────
    {
        "name": "set_allowance",
        "description": "Set up weekly allowance for a child.",
        "input_schema": {
            "type": "object",
            "properties": {
                "child_name": {"type": "string"},
                "weekly_amount": {"type": "number"},
                "day_of_week": {"type": "string", "description": "e.g. Saturday, Friday"}
            },
            "required": ["child_name", "weekly_amount"]
        }
    },
    {
        "name": "pay_allowance",
        "description": "Pay a child their allowance.",
        "input_schema": {
            "type": "object",
            "properties": {
                "child_name": {"type": "string"},
                "amount": {"type": "number", "description": "Override amount, or omit to use weekly amount"},
                "notes": {"type": "string"}
            },
            "required": ["child_name"]
        }
    },
    {
        "name": "spend_allowance",
        "description": "Record a child spending their allowance money.",
        "input_schema": {
            "type": "object",
            "properties": {
                "child_name": {"type": "string"},
                "amount": {"type": "number"},
                "description": {"type": "string"}
            },
            "required": ["child_name", "amount", "description"]
        }
    },
    {
        "name": "get_allowance_balance",
        "description": "Check a child's allowance balance.",
        "input_schema": {
            "type": "object",
            "properties": {
                "child_name": {"type": "string"}
            }
        }
    },

    # ── BEDTIME STORIES ────────────────────────────────────────────────────
    {
        "name": "tell_bedtime_story",
        "description": "Tell a bedtime story. Use when someone asks for a bedtime story, a story for their kid, or 'make up a story about...'",
        "input_schema": {
            "type": "object",
            "properties": {
                "child_name": {"type": "string", "description": "Who the story is for"},
                "theme": {"type": "string", "description": "e.g. dragons, space, princesses, animals, adventure"},
                "characters": {"type": "string", "description": "Specific characters to include"},
                "age": {"type": "string", "description": "Child's age, e.g. '5', '8-10'"},
                "save_story": {"type": "boolean", "description": "Save this story for later"}
            }
        }
    },
    {
        "name": "get_saved_stories",
        "description": "Get previously saved bedtime stories.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search by character or theme"}
            }
        }
    },

    # ── WEB SEARCH ─────────────────────────────────────────────────────────
    {
        "name": "search_web",
        "description": (
            "Search the internet for current information, how-to guides, product info, "
            "local services, or anything that requires up-to-date knowledge. "
            "Use when the user asks something that needs real-world or recent information. "
            "Safe search is always on — results are family-friendly."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "What to search for"},
                "search_type": {
                    "type": "string",
                    "enum": ["web", "news", "wikipedia"],
                    "description": "web = general search, news = current events, wikipedia = factual/educational topics"
                }
            },
            "required": ["query"]
        }
    },

    # ── FULL SYSTEM TEST ───────────────────────────────────────────────────
    {
        "name": "run_system_test",
        "description": "Run a complete self-test of every single module and tool. Use when asked to test yourself, check all your capabilities, run a full diagnostic, or verify everything is working.",
        "input_schema": {"type": "object", "properties": {}}
    },

    # ── SELF-HEALTH & REPAIR ───────────────────────────────────────────────
    {
        "name": "check_my_health",
        "description": "Run a full self-diagnostic — check skill files, memory integrity, profile data. Use when asked 'how are you feeling', 'are you ok', 'run a self-check', or anytime you want to verify you're healthy.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "take_snapshot",
        "description": "Save a snapshot of your current state as a restore point. Use before making big changes, when asked to 'save your state', or any time you want a safety checkpoint.",
        "input_schema": {
            "type": "object",
            "properties": {
                "label": {"type": "string", "description": "A short label for this snapshot, e.g. 'before_update', 'healthy_state'"}
            }
        }
    },
    {
        "name": "list_snapshots",
        "description": "List all available restore points/snapshots.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "rollback",
        "description": "Restore yourself to a previous healthy state. Use when something is broken and you need to go back to a known-good point.",
        "input_schema": {
            "type": "object",
            "properties": {
                "snapshot_name": {"type": "string", "description": "Specific snapshot to restore, or omit to use the most recent healthy one"}
            }
        }
    },
    {
        "name": "repair_myself",
        "description": "Attempt to repair any corrupted or broken files by restoring them from the most recent snapshot. Use when you detect something is wrong with your data.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "get_my_identity",
        "description": "Get your unique identity information — instance ID, QR code details, version. Use when asked who you are, what your ID is, or about your QR code.",
        "input_schema": {"type": "object", "properties": {}}
    },

    # ── PERMANENT MEMORY ───────────────────────────────────────────────────
    {
        "name": "remember",
        "description": (
            "Save something important about your owner to your permanent memory. "
            "Use this immediately whenever you learn: their name, family members, job, where they live, "
            "hobbies, health info, goals, preferences, or anything else significant about their life. "
            "You MUST call this tool the moment someone shares personal information — don't wait."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "fact": {
                    "type": "string",
                    "description": "The fact to remember, written clearly. e.g. 'Owner's name is Frank', 'Has two kids named Emma and Jack', 'Works as a nurse'"
                },
                "category": {
                    "type": "string",
                    "description": "Category: person, family, work, health, preference, goal, home, pet, or general"
                }
            },
            "required": ["fact", "category"]
        }
    },
    {
        "name": "recall",
        "description": "Search your permanent memory for facts about your owner. Use when you're not sure if you already know something.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "What to search for, e.g. 'kids', 'job', 'health'"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "update_profile",
        "description": "Update the owner's name or Orby's name in the profile.",
        "input_schema": {
            "type": "object",
            "properties": {
                "owner_name": {"type": "string", "description": "The owner's first name"},
                "orby_name": {"type": "string", "description": "What they want to call their Orby"}
            }
        }
    },

    # ── MODULE MANAGEMENT ──────────────────────────────────────────────────
    {
        "name": "list_modules",
        "description": (
            "Show all installed modules, available add-ons, and coming-soon features. "
            "Use when the owner asks what you can do, what modules exist, what's available to add, "
            "or what paid features exist."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "filter": {
                    "type": "string",
                    "enum": ["all", "free", "paid"],
                    "description": "Filter by tier. Default: all."
                }
            }
        }
    },
    {
        "name": "get_module_info",
        "description": "Get full details about a specific module — what it does, price, tools it adds.",
        "input_schema": {
            "type": "object",
            "properties": {
                "module_id": {
                    "type": "string",
                    "description": "Module ID or name, e.g. 'smart_home', 'Smart Home Control'"
                }
            },
            "required": ["module_id"]
        }
    },
    {
        "name": "check_for_updates",
        "description": "Check if any installed modules have updates available.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "request_module",
        "description": (
            "Express interest in purchasing or activating a module. Use when the owner asks to buy, "
            "add, or get a paid module, or asks how to unlock a feature."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "module_id": {
                    "type": "string",
                    "description": "Module ID or name to request, e.g. 'smart_home'"
                }
            },
            "required": ["module_id"]
        }
    },
    {
        "name": "activate_module",
        "description": "Activate a module using a license key from a purchase.",
        "input_schema": {
            "type": "object",
            "properties": {
                "module_id": {"type": "string", "description": "Module ID to activate"},
                "license_key": {"type": "string", "description": "License key from purchase email"}
            },
            "required": ["module_id"]
        }
    },

    # ── PRODUCT DEVELOPMENT ────────────────────────────────────────────────
    {
        "name": "save_product_idea",
        "description": (
            "Capture a product idea instantly. Use the moment someone shares a feature idea, "
            "product concept, improvement thought, or 'what if we...' moment. Never wait — capture it now."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "idea": {"type": "string", "description": "The idea, described clearly"},
                "product": {"type": "string", "description": "Which product it's for (optional)"}
            },
            "required": ["idea"]
        }
    },
    {
        "name": "get_product_ideas",
        "description": "Get saved product ideas, optionally filtered by product or status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product": {"type": "string", "description": "Filter by product name"},
                "status": {
                    "type": "string",
                    "enum": ["new", "reviewed", "added_to_roadmap", "rejected"],
                    "description": "Filter by idea status. Default: all."
                }
            }
        }
    },
    {
        "name": "update_idea_status",
        "description": "Update the status of a product idea — reviewed, added to roadmap, or rejected.",
        "input_schema": {
            "type": "object",
            "properties": {
                "idea_id": {"type": "string", "description": "Idea ID from get_product_ideas"},
                "status": {
                    "type": "string",
                    "enum": ["new", "reviewed", "added_to_roadmap", "rejected"]
                }
            },
            "required": ["idea_id", "status"]
        }
    },
    {
        "name": "add_product",
        "description": "Create a product entry to track. Use when starting a new product or project.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name":            {"type": "string", "description": "Product name"},
                "description":     {"type": "string", "description": "What this product is"},
                "status":          {"type": "string", "enum": ["idea", "in_development", "launched", "paused"], "description": "Default: in_development"},
                "target_audience": {"type": "string", "description": "Who this is for, e.g. 'families', 'contractors', 'medical practices'"},
                "revenue_model":   {"type": "string", "description": "How it makes money, e.g. 'subscription $9.99/mo', 'B2B license', 'one-time purchase'"},
                "industry_tags":   {"type": "string", "description": "Comma-separated industries, e.g. 'construction, medical, legal'"},
                "linked_modules":  {"type": "string", "description": "Comma-separated Orby modules this product uses or requires"}
            },
            "required": ["name"]
        }
    },
    {
        "name": "update_product",
        "description": "Update a product's details — audience, revenue model, industry tags, linked modules.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_id":      {"type": "string", "description": "Product ID or name"},
                "description":     {"type": "string"},
                "target_audience": {"type": "string"},
                "revenue_model":   {"type": "string"},
                "industry_tags":   {"type": "string", "description": "Comma-separated industries"},
                "linked_modules":  {"type": "string", "description": "Comma-separated module names"}
            },
            "required": ["product_id"]
        }
    },
    {
        "name": "get_products",
        "description": "List all tracked products with full details — audience, revenue model, feature counts.",
        "input_schema": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["idea", "in_development", "launched", "paused"],
                    "description": "Filter by status (optional)"
                }
            }
        }
    },
    {
        "name": "get_product_dashboard",
        "description": (
            "Get the full product status dashboard — all products at a glance with feature counts, "
            "launch checklist progress, unreviewed ideas, and revenue model. "
            "Use when asked for a product overview or 'where do things stand'."
        ),
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "update_product_status",
        "description": "Update the status of a product — e.g. mark it as launched.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_id": {"type": "string", "description": "Product ID or name"},
                "status": {
                    "type": "string",
                    "enum": ["idea", "in_development", "launched", "paused"]
                }
            },
            "required": ["product_id", "status"]
        }
    },
    {
        "name": "add_product_note",
        "description": "Save a note tied to a specific product — decisions, context, anything worth remembering about this product.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_id": {"type": "string", "description": "Product ID or name"},
                "content": {"type": "string", "description": "The note content"}
            },
            "required": ["product_id", "content"]
        }
    },
    {
        "name": "get_product_notes",
        "description": "Get all notes saved for a specific product.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_id": {"type": "string", "description": "Product ID or name"}
            },
            "required": ["product_id"]
        }
    },
    {
        "name": "add_feature",
        "description": (
            "Add a feature to a product's roadmap. Tracks what's built, what's planned, "
            "and whether it's free or paid."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "product": {"type": "string", "description": "Product name this feature belongs to"},
                "name": {"type": "string", "description": "Feature name"},
                "description": {"type": "string", "description": "What this feature does"},
                "tier": {
                    "type": "string",
                    "enum": ["free", "paid"],
                    "description": "Free tier or paid add-on. Default: free"
                },
                "status": {
                    "type": "string",
                    "enum": ["built", "in_progress", "planned", "backlog"],
                    "description": "Current build status. Default: planned"
                },
                "priority": {
                    "type": "string",
                    "enum": ["high", "normal", "low"],
                    "description": "Default: normal"
                }
            },
            "required": ["product", "name"]
        }
    },
    {
        "name": "update_feature_status",
        "description": "Update the build status of a feature — e.g. mark it as built or in progress.",
        "input_schema": {
            "type": "object",
            "properties": {
                "feature_id": {"type": "string", "description": "Feature ID or name"},
                "status": {
                    "type": "string",
                    "enum": ["built", "in_progress", "planned", "backlog"]
                }
            },
            "required": ["feature_id", "status"]
        }
    },
    {
        "name": "get_features",
        "description": "Get features for a product, optionally filtered by status or tier.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product": {"type": "string", "description": "Product name (optional — omit for all products)"},
                "status": {
                    "type": "string",
                    "enum": ["built", "in_progress", "planned", "backlog"]
                },
                "tier": {
                    "type": "string",
                    "enum": ["free", "paid"]
                }
            }
        }
    },
    {
        "name": "get_product_roadmap",
        "description": (
            "Get the full roadmap view for one or all products — status, feature breakdown by tier, "
            "what's built vs. planned, and launch checklist progress."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "product": {"type": "string", "description": "Product name to focus on (optional — omit for all)"}
            }
        }
    },
    {
        "name": "add_launch_checklist",
        "description": "Create a launch checklist for a product.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product": {"type": "string", "description": "Product name"},
                "name": {"type": "string", "description": "Checklist name, e.g. 'v1.0 Launch'"},
                "items": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of checklist items to pre-populate"
                }
            },
            "required": ["product", "name"]
        }
    },
    {
        "name": "add_checklist_item",
        "description": "Add an item to an existing launch checklist.",
        "input_schema": {
            "type": "object",
            "properties": {
                "checklist_id": {"type": "string", "description": "Checklist ID or name"},
                "task": {"type": "string", "description": "The task to add"}
            },
            "required": ["checklist_id", "task"]
        }
    },
    {
        "name": "complete_checklist_item",
        "description": "Check off a launch checklist item as done.",
        "input_schema": {
            "type": "object",
            "properties": {
                "checklist_id": {"type": "string", "description": "Checklist ID or name"},
                "item_id": {"type": "string", "description": "Item ID or part of the task text"}
            },
            "required": ["checklist_id", "item_id"]
        }
    },
    {
        "name": "get_launch_checklist",
        "description": "Get a launch checklist with all items and completion status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "checklist_id": {"type": "string", "description": "Checklist ID or name. Omit to list all checklists."}
            }
        }
    },

    # ── DEEP MEMORY ────────────────────────────────────────────────────────
    {
        "name": "save_context_note",
        "description": (
            "Save a detailed context note — full specs, background, how something works, "
            "why a decision was made, complete technical details. No truncation. "
            "Use this when something is too detailed for 'remember'. "
            "Use it when you need to capture a full conversation's worth of context about a topic."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "topic":   {"type": "string", "description": "Short name for this context, e.g. 'Orby memory architecture', 'Bridge server design'"},
                "content": {"type": "string", "description": "The full detailed content — write everything, don't summarize"},
                "project": {"type": "string", "description": "Product or project this belongs to (optional)"},
                "tags":    {"type": "string", "description": "Comma-separated tags (optional)"}
            },
            "required": ["topic", "content"]
        }
    },
    {
        "name": "get_context_note",
        "description": "Retrieve a full context note by topic name or ID.",
        "input_schema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string", "description": "Topic name, ID, or keyword to find"}
            },
            "required": ["topic"]
        }
    },
    {
        "name": "search_context",
        "description": "Search context notes by keyword or project.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query":   {"type": "string", "description": "Keyword to search for"},
                "project": {"type": "string", "description": "Narrow to a specific project (optional)"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "list_context_notes",
        "description": "List all saved context notes, optionally filtered by project.",
        "input_schema": {
            "type": "object",
            "properties": {
                "project": {"type": "string", "description": "Filter by project (optional)"}
            }
        }
    },
    {
        "name": "delete_context_note",
        "description": "Delete a context note by ID or topic name.",
        "input_schema": {
            "type": "object",
            "properties": {
                "note_id": {"type": "string", "description": "Note ID or exact topic name"}
            },
            "required": ["note_id"]
        }
    },
    {
        "name": "log_decision",
        "description": (
            "Log a decision with full reasoning. Use whenever a real decision is made — "
            "what was chosen, why, and what alternatives were considered. "
            "This is what answers 'why did we do it that way?' months later."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "decision":     {"type": "string", "description": "What was decided, stated clearly"},
                "reasoning":    {"type": "string", "description": "Why this decision was made — full reasoning"},
                "project":      {"type": "string", "description": "Product or project this belongs to (optional)"},
                "alternatives": {"type": "string", "description": "Other options that were considered and rejected (optional)"}
            },
            "required": ["decision", "reasoning"]
        }
    },
    {
        "name": "get_decisions",
        "description": "Retrieve logged decisions, optionally filtered by project or keyword.",
        "input_schema": {
            "type": "object",
            "properties": {
                "project": {"type": "string", "description": "Filter by project name"},
                "query":   {"type": "string", "description": "Search keyword"}
            }
        }
    },
    {
        "name": "save_session_summary",
        "description": (
            "Save a complete summary of this conversation or work session. "
            "Use at the end of any significant session so nothing gets lost between conversations. "
            "Capture what was discussed, what was built, decisions made, and next steps — in full detail."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "title":       {"type": "string", "description": "Short title for this session, e.g. 'Built module management system'"},
                "what_we_did": {"type": "string", "description": "Full account of what was discussed and done — be thorough"},
                "decisions":   {"type": "string", "description": "Key decisions made during the session"},
                "next_steps":  {"type": "string", "description": "What comes next after this session"},
                "project":     {"type": "string", "description": "Product or project this session was about (optional)"}
            },
            "required": ["title", "what_we_did"]
        }
    },
    {
        "name": "get_session",
        "description": "Retrieve a saved session summary by title or ID.",
        "input_schema": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string", "description": "Session ID or title keyword"}
            },
            "required": ["session_id"]
        }
    },
    {
        "name": "list_sessions",
        "description": "List all saved session summaries, most recent first.",
        "input_schema": {
            "type": "object",
            "properties": {
                "project": {"type": "string", "description": "Filter by project (optional)"},
                "limit":   {"type": "integer", "description": "Max results (default 20)"}
            }
        }
    },
    {
        "name": "search_sessions",
        "description": "Search session summaries by keyword.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Keyword to search for across all sessions"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "search_everything",
        "description": (
            "Search across ALL deep memory — context notes, decisions, session summaries, and specs — in one call. "
            "Use when you need to find something and don't know where it was saved."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "What to search for"}
            },
            "required": ["query"]
        }
    },

    # ── BUILD SPECS ────────────────────────────────────────────────────────
    {
        "name": "save_spec",
        "description": (
            "Write a complete, build-ready technical spec for a new module or feature. "
            "Claude Code can read this spec directly from the file and build from it — "
            "no copying, no pasting. Use when Frank describes something he wants built. "
            "Write every section in full detail — the more complete, the better the build."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Short name for this spec, e.g. 'Email Assistant Module', 'Invoice Generator'"
                },
                "purpose": {
                    "type": "string",
                    "description": "What this module does, who it's for, why it's needed. 2-4 sentences."
                },
                "data_model": {
                    "type": "string",
                    "description": (
                        "What data gets stored. For each data type: field names, types, what they mean. "
                        "Include the JSON filename it should save to (e.g. profiles/{name}/skills/emails.json). "
                        "Be specific — this is what the developer uses to create the data structures."
                    )
                },
                "tools": {
                    "type": "string",
                    "description": (
                        "Every tool to build. For each tool: name, what it does, required inputs, "
                        "optional inputs, what it returns. Include edge cases and error behavior."
                    )
                },
                "file_path": {
                    "type": "string",
                    "description": "Where the skill file should be created, e.g. skills/email_skill.py"
                },
                "behavior_rules": {
                    "type": "string",
                    "description": (
                        "How Orby should use this module. When to call which tools automatically, "
                        "what triggers each tool, what to say to the user. This becomes part of the system prompt."
                    )
                },
                "test_notes": {
                    "type": "string",
                    "description": "How to verify the module works — what to test, what good output looks like"
                },
                "project": {
                    "type": "string",
                    "description": "Which product this spec belongs to (optional)"
                }
            },
            "required": ["title", "purpose", "data_model", "tools", "file_path"]
        }
    },
    {
        "name": "get_spec",
        "description": "Retrieve a full build spec by title or ID.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Spec title or ID"}
            },
            "required": ["title"]
        }
    },
    {
        "name": "list_specs",
        "description": "List all saved specs and their build status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "project": {"type": "string", "description": "Filter by project (optional)"}
            }
        }
    },
    {
        "name": "update_spec_status",
        "description": "Update the status of a spec — mark it as built, in progress, or back to draft.",
        "input_schema": {
            "type": "object",
            "properties": {
                "spec_id": {"type": "string", "description": "Spec ID or title"},
                "status": {
                    "type": "string",
                    "enum": ["draft", "ready_to_build", "in_progress", "built"]
                }
            },
            "required": ["spec_id", "status"]
        }
    },

    # ── SOCIAL MEDIA ───────────────────────────────────────────────────────
    {
        "name": "get_social_setup_guide",
        "description": "Get step-by-step instructions for connecting a social media account — Facebook, Instagram, Twitter, LinkedIn, or TikTok.",
        "input_schema": {
            "type": "object",
            "properties": {
                "platform": {"type": "string", "description": "facebook, instagram, twitter, linkedin, or tiktok"}
            },
            "required": ["platform"]
        }
    },
    {
        "name": "connect_social_account",
        "description": "Connect a social media account by saving its API credentials. Use after the owner has followed the setup guide and has their tokens.",
        "input_schema": {
            "type": "object",
            "properties": {
                "platform": {"type": "string", "description": "facebook, instagram, twitter, linkedin, or tiktok"},
                "credentials": {
                    "type": "object",
                    "description": "Platform credentials object. Facebook: {page_id, access_token}. Instagram: {ig_user_id, access_token}. Twitter: {api_key, api_secret, access_token, access_token_secret, username}. LinkedIn: {access_token, person_urn}."
                }
            },
            "required": ["platform", "credentials"]
        }
    },
    {
        "name": "get_social_accounts",
        "description": "List all connected social media accounts.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "disconnect_social_account",
        "description": "Disconnect a social media account.",
        "input_schema": {
            "type": "object",
            "properties": {
                "platform": {"type": "string"}
            },
            "required": ["platform"]
        }
    },
    {
        "name": "setup_page_profile",
        "description": "Build out a social media page profile — bio, website, phone, email, description, hours. Use when setting up a page for the first time or updating it.",
        "input_schema": {
            "type": "object",
            "properties": {
                "platform":    {"type": "string"},
                "bio":         {"type": "string", "description": "Short bio or about text"},
                "website":     {"type": "string"},
                "phone":       {"type": "string"},
                "email":       {"type": "string"},
                "hours":       {"type": "string"},
                "description": {"type": "string", "description": "Longer description"}
            },
            "required": ["platform"]
        }
    },
    {
        "name": "create_post",
        "description": "Post content to one or more social media platforms right now. Always show the owner the content and get confirmation before calling this tool.",
        "input_schema": {
            "type": "object",
            "properties": {
                "content":   {"type": "string", "description": "The post text"},
                "platforms": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Platforms to post to. Omit to post to all connected accounts."
                },
                "image_url": {"type": "string", "description": "Publicly accessible image URL (required for Instagram)"}
            },
            "required": ["content"]
        }
    },
    {
        "name": "schedule_post",
        "description": "Schedule a post to go out at a specific time on one or more platforms.",
        "input_schema": {
            "type": "object",
            "properties": {
                "content":    {"type": "string"},
                "publish_at": {"type": "string", "description": "ISO 8601 datetime, e.g. 2026-05-16T09:00:00"},
                "platforms":  {"type": "array", "items": {"type": "string"}},
                "image_url":  {"type": "string"}
            },
            "required": ["content", "publish_at"]
        }
    },
    {
        "name": "get_scheduled_posts",
        "description": "See all scheduled social media posts waiting to go out.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "cancel_scheduled_post",
        "description": "Cancel a scheduled social media post before it goes out.",
        "input_schema": {
            "type": "object",
            "properties": {
                "post_id": {"type": "string", "description": "Post ID from get_scheduled_posts"}
            },
            "required": ["post_id"]
        }
    },
    {
        "name": "get_post_history",
        "description": "See past social media posts Orby has published.",
        "input_schema": {
            "type": "object",
            "properties": {
                "platform": {"type": "string", "description": "Filter by platform (optional)"},
                "limit":    {"type": "integer", "description": "Max posts to show (default 10)"}
            }
        }
    },
    {
        "name": "save_post_draft",
        "description": "Save a social media post as a draft without publishing it.",
        "input_schema": {
            "type": "object",
            "properties": {
                "content":   {"type": "string"},
                "platforms": {"type": "array", "items": {"type": "string"}},
                "notes":     {"type": "string", "description": "Internal notes about this draft"}
            },
            "required": ["content"]
        }
    },
    {
        "name": "get_post_drafts",
        "description": "See all saved social media post drafts.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "delete_post_draft",
        "description": "Delete a saved draft.",
        "input_schema": {
            "type": "object",
            "properties": {
                "draft_id": {"type": "string"}
            },
            "required": ["draft_id"]
        }
    },
    {
        "name": "get_social_analytics",
        "description": "Get follower count and engagement analytics for a social media platform.",
        "input_schema": {
            "type": "object",
            "properties": {
                "platform": {"type": "string"}
            },
            "required": ["platform"]
        }
    },
    {
        "name": "get_page_info",
        "description": "Get the current profile info for a connected social media page.",
        "input_schema": {
            "type": "object",
            "properties": {
                "platform": {"type": "string"}
            },
            "required": ["platform"]
        }
    },

    # ── AI IMAGE STUDIO ────────────────────────────────────────────────────
    {
        "name": "generate_image",
        "description": "Generate an image from a text description using AI. Use when user wants to create artwork, illustrations, logos, photos, or any visual content.",
        "input_schema": {
            "type": "object",
            "properties": {
                "prompt": {"type": "string", "description": "Detailed description of the image to generate"},
                "style": {"type": "string", "description": "Optional style: 'photorealistic', 'illustration', 'cartoon', 'watercolor', 'oil painting', 'minimalist', 'logo', etc."},
                "size": {"type": "string", "description": "Image size: '1024x1024' (square), '1792x1024' (landscape), '1024x1792' (portrait). Default: 1024x1024"},
                "quality": {"type": "string", "description": "'standard' or 'hd'. Default: standard"}
            },
            "required": ["prompt"]
        }
    },
    {
        "name": "get_image_library",
        "description": "View the user's saved AI-generated images.",
        "input_schema": {"type": "object", "properties": {"limit": {"type": "integer"}}}
    },
    {
        "name": "delete_image_project",
        "description": "Delete a saved image project by ID.",
        "input_schema": {
            "type": "object",
            "properties": {"project_id": {"type": "string"}},
            "required": ["project_id"]
        }
    },
    {
        "name": "set_image_api_key",
        "description": "Save the user's OpenAI API key for image generation.",
        "input_schema": {
            "type": "object",
            "properties": {"api_key": {"type": "string", "description": "OpenAI API key starting with sk-"}},
            "required": ["api_key"]
        }
    },
    {
        "name": "get_image_api_status",
        "description": "Check if the image generation API key is configured.",
        "input_schema": {"type": "object", "properties": {}}
    },

    # ── 3D CREATOR ─────────────────────────────────────────────────────────
    {
        "name": "generate_3d",
        "description": "Generate a 3D model or render from a text description. Use for product mockups, objects, characters, scenes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "prompt": {"type": "string", "description": "Description of the 3D object or scene to generate"},
                "art_style": {"type": "string", "description": "'realistic', 'cartoon', 'low-poly', 'sculpture', or 'pbr'. Default: realistic"},
                "negative_prompt": {"type": "string", "description": "What to avoid in the generation"}
            },
            "required": ["prompt"]
        }
    },
    {
        "name": "check_3d_job",
        "description": "Check the status of a 3D generation job. Use when user asks if their 3D model is ready.",
        "input_schema": {
            "type": "object",
            "properties": {"task_id": {"type": "string"}},
            "required": ["task_id"]
        }
    },
    {
        "name": "get_3d_library",
        "description": "View the user's saved 3D models and renders.",
        "input_schema": {"type": "object", "properties": {"limit": {"type": "integer"}}}
    },
    {
        "name": "delete_3d_project",
        "description": "Delete a saved 3D project by ID.",
        "input_schema": {
            "type": "object",
            "properties": {"project_id": {"type": "string"}},
            "required": ["project_id"]
        }
    },
    {
        "name": "set_3d_api_key",
        "description": "Save the user's Meshy.ai API key for 3D generation.",
        "input_schema": {
            "type": "object",
            "properties": {"api_key": {"type": "string"}},
            "required": ["api_key"]
        }
    },
    {
        "name": "get_3d_api_status",
        "description": "Check if the 3D generation API key is configured.",
        "input_schema": {"type": "object", "properties": {}}
    },

    # ── AI VIDEO STUDIO ────────────────────────────────────────────────────
    {
        "name": "generate_video",
        "description": "Generate a short video clip or animation from a text description using AI. Use for social media clips, website backgrounds, short films, animations.",
        "input_schema": {
            "type": "object",
            "properties": {
                "prompt": {"type": "string", "description": "Detailed description of the video to generate — describe the scene, motion, mood, camera movement"},
                "duration": {"type": "integer", "description": "Video length in seconds: 5 or 10. Default: 5"},
                "ratio": {"type": "string", "description": "Aspect ratio: '1280:720' (landscape/YouTube), '720:1280' (vertical/TikTok), '960:960' (square). Default: 1280:720"}
            },
            "required": ["prompt"]
        }
    },
    {
        "name": "check_video_job",
        "description": "Check if an AI video generation job is complete. Use when user asks if their video is ready.",
        "input_schema": {
            "type": "object",
            "properties": {"task_id": {"type": "string"}},
            "required": ["task_id"]
        }
    },
    {
        "name": "get_video_library",
        "description": "View the user's saved AI-generated videos.",
        "input_schema": {"type": "object", "properties": {"limit": {"type": "integer"}}}
    },
    {
        "name": "delete_video_project",
        "description": "Delete a saved video project by ID.",
        "input_schema": {
            "type": "object",
            "properties": {"project_id": {"type": "string"}},
            "required": ["project_id"]
        }
    },
    {
        "name": "set_video_api_key",
        "description": "Save the user's Runway ML API key for video generation.",
        "input_schema": {
            "type": "object",
            "properties": {"api_key": {"type": "string"}},
            "required": ["api_key"]
        }
    },
    {
        "name": "get_video_api_status",
        "description": "Check if the video generation API key is configured.",
        "input_schema": {"type": "object", "properties": {}}
    },

    # ── ONBOARDING ─────────────────────────────────────────────────────────
    {
        "name": "scan_website",
        "description": "Fetch and read the content of a website URL so Orby can learn about the user's business. Use during onboarding when user provides their website.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "The website URL to scan"}
            },
            "required": ["url"]
        }
    },
    {
        "name": "complete_onboarding",
        "description": "Mark first-run onboarding as complete. Call this after the user's name is saved and the website question has been handled (whether they provided one or not).",
        "input_schema": {"type": "object", "properties": {}}
    },

    # ── I001: LEGAL PRO ────────────────────────────────────────────────────
    {
        "name": "add_legal_case",
        "description": "Add a new legal case for a client. Use when starting to track a new matter.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"},
                "case_type": {"type": "string", "description": "e.g. personal injury, contract dispute, family law"},
                "description": {"type": "string"},
                "status": {"type": "string", "description": "open, closed, pending. Default: open"}
            },
            "required": ["client_name", "case_type", "description"]
        }
    },
    {
        "name": "list_legal_cases",
        "description": "List all legal cases, optionally filtered by status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "description": "Filter by status: open, closed, pending. Leave blank for all."}
            }
        }
    },
    {
        "name": "update_legal_case",
        "description": "Update a legal case's status, description, or add a note.",
        "input_schema": {
            "type": "object",
            "properties": {
                "case_id": {"type": "string"},
                "status": {"type": "string", "description": "New status: open, closed, pending"},
                "notes": {"type": "string", "description": "Note to append to the case"},
                "description": {"type": "string", "description": "Updated description"}
            },
            "required": ["case_id"]
        }
    },
    {
        "name": "add_case_deadline",
        "description": "Add a deadline to a legal case.",
        "input_schema": {
            "type": "object",
            "properties": {
                "case_id": {"type": "string"},
                "deadline_date": {"type": "string", "description": "YYYY-MM-DD"},
                "description": {"type": "string", "description": "What this deadline is for"}
            },
            "required": ["case_id", "deadline_date", "description"]
        }
    },
    {
        "name": "log_billable_time",
        "description": "Log billable hours to a legal case.",
        "input_schema": {
            "type": "object",
            "properties": {
                "case_id": {"type": "string"},
                "hours": {"type": "number", "description": "Hours worked, e.g. 1.5"},
                "description": {"type": "string", "description": "What was done"},
                "date": {"type": "string", "description": "YYYY-MM-DD, defaults to today"}
            },
            "required": ["case_id", "hours", "description"]
        }
    },
    {
        "name": "get_billing_summary",
        "description": "Get total billable hours and calculated amount for one case or all cases.",
        "input_schema": {
            "type": "object",
            "properties": {
                "case_id": {"type": "string", "description": "Specific case ID, or leave blank for all cases"}
            }
        }
    },
    {
        "name": "add_case_note",
        "description": "Append a note to a legal case.",
        "input_schema": {
            "type": "object",
            "properties": {
                "case_id": {"type": "string"},
                "note": {"type": "string"}
            },
            "required": ["case_id", "note"]
        }
    },
    {
        "name": "get_legal_case",
        "description": "Get full details of a legal case including deadlines, time entries, and notes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "case_id": {"type": "string"}
            },
            "required": ["case_id"]
        }
    },

    # ── I002: MEDICAL PRO ──────────────────────────────────────────────────
    {
        "name": "add_patient",
        "description": "Add a patient to the practice. HIPAA-conscious — data stored locally only.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "dob": {"type": "string", "description": "Date of birth, YYYY-MM-DD"},
                "phone": {"type": "string"},
                "email": {"type": "string"},
                "insurance": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["name", "dob", "phone"]
        }
    },
    {
        "name": "list_patients",
        "description": "List patients in the practice, optionally filtered by name search.",
        "input_schema": {
            "type": "object",
            "properties": {
                "search": {"type": "string", "description": "Search by name (partial match)"}
            }
        }
    },
    {
        "name": "add_medical_appointment",
        "description": "Schedule a medical appointment for a patient.",
        "input_schema": {
            "type": "object",
            "properties": {
                "patient_name": {"type": "string"},
                "date": {"type": "string", "description": "YYYY-MM-DD"},
                "time": {"type": "string", "description": "HH:MM"},
                "type": {"type": "string", "description": "e.g. follow-up, new patient, procedure"},
                "notes": {"type": "string"}
            },
            "required": ["patient_name", "date", "time", "type"]
        }
    },
    {
        "name": "list_medical_appointments",
        "description": "List medical appointments, optionally filtered by date.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date": {"type": "string", "description": "YYYY-MM-DD for a specific date"},
                "upcoming_only": {"type": "boolean", "description": "Show only future appointments (default true)"}
            }
        }
    },
    {
        "name": "add_treatment_note",
        "description": "Add a treatment or SOAP note for a patient. Stored locally only.",
        "input_schema": {
            "type": "object",
            "properties": {
                "patient_name": {"type": "string"},
                "date": {"type": "string", "description": "YYYY-MM-DD"},
                "note": {"type": "string", "description": "Treatment note content"},
                "provider": {"type": "string", "description": "Provider name (optional)"}
            },
            "required": ["patient_name", "date", "note"]
        }
    },
    {
        "name": "get_patient_history",
        "description": "Get full history for a patient — appointments, treatment notes, and prescriptions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "patient_name": {"type": "string"}
            },
            "required": ["patient_name"]
        }
    },
    {
        "name": "add_prescription",
        "description": "Log a prescription for a patient.",
        "input_schema": {
            "type": "object",
            "properties": {
                "patient_name": {"type": "string"},
                "medication": {"type": "string"},
                "dosage": {"type": "string", "description": "e.g. 10mg twice daily"},
                "prescriber": {"type": "string"},
                "date": {"type": "string", "description": "YYYY-MM-DD, defaults to today"}
            },
            "required": ["patient_name", "medication", "dosage", "prescriber"]
        }
    },
    {
        "name": "list_prescriptions",
        "description": "List all prescriptions on file for a patient.",
        "input_schema": {
            "type": "object",
            "properties": {
                "patient_name": {"type": "string"}
            },
            "required": ["patient_name"]
        }
    },

    # ── I003: REAL ESTATE PRO ──────────────────────────────────────────────
    {
        "name": "add_listing",
        "description": "Add a real estate listing to track.",
        "input_schema": {
            "type": "object",
            "properties": {
                "address": {"type": "string"},
                "price": {"type": "number"},
                "bedrooms": {"type": "integer"},
                "bathrooms": {"type": "number"},
                "sqft": {"type": "integer"},
                "status": {"type": "string", "description": "active, pending, sold. Default: active"},
                "notes": {"type": "string"}
            },
            "required": ["address", "price", "bedrooms", "bathrooms", "sqft"]
        }
    },
    {
        "name": "list_listings",
        "description": "List real estate listings, optionally filtered by status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "description": "active, pending, or sold"}
            }
        }
    },
    {
        "name": "update_listing",
        "description": "Update a listing's status, price, or notes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "listing_id": {"type": "string"},
                "status": {"type": "string", "description": "active, pending, sold"},
                "price": {"type": "number", "description": "New price"},
                "notes": {"type": "string"}
            },
            "required": ["listing_id"]
        }
    },
    {
        "name": "add_re_client",
        "description": "Add a real estate client — buyer or seller.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "type": {"type": "string", "description": "buyer or seller"},
                "phone": {"type": "string"},
                "email": {"type": "string"},
                "budget": {"type": "string", "description": "Budget range, e.g. '$400k-$500k'"},
                "notes": {"type": "string"}
            },
            "required": ["name", "type", "phone"]
        }
    },
    {
        "name": "list_re_clients",
        "description": "List real estate clients, optionally filtered by buyer or seller.",
        "input_schema": {
            "type": "object",
            "properties": {
                "type": {"type": "string", "description": "buyer or seller"}
            }
        }
    },
    {
        "name": "add_showing",
        "description": "Schedule a property showing for a client.",
        "input_schema": {
            "type": "object",
            "properties": {
                "listing_id": {"type": "string"},
                "client_name": {"type": "string"},
                "date": {"type": "string", "description": "YYYY-MM-DD"},
                "time": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["listing_id", "client_name", "date", "time"]
        }
    },
    {
        "name": "list_showings",
        "description": "List scheduled property showings.",
        "input_schema": {
            "type": "object",
            "properties": {
                "upcoming_only": {"type": "boolean", "description": "Show only future showings (default true)"}
            }
        }
    },
    {
        "name": "add_offer",
        "description": "Record an offer on a listing.",
        "input_schema": {
            "type": "object",
            "properties": {
                "listing_id": {"type": "string"},
                "client_name": {"type": "string"},
                "amount": {"type": "number"},
                "date": {"type": "string", "description": "YYYY-MM-DD"},
                "status": {"type": "string", "description": "pending, accepted, rejected. Default: pending"},
                "notes": {"type": "string"}
            },
            "required": ["listing_id", "client_name", "amount", "date"]
        }
    },
    {
        "name": "calculate_commission",
        "description": "Calculate real estate commission for a sale.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sale_price": {"type": "number"},
                "commission_rate": {"type": "number", "description": "Commission percentage, default 6.0"}
            },
            "required": ["sale_price"]
        }
    },

    # ── I004: RESTAURANT PRO ───────────────────────────────────────────────
    {
        "name": "add_menu_item",
        "description": "Add an item to the restaurant menu.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "category": {"type": "string", "description": "e.g. Appetizers, Entrees, Desserts, Drinks"},
                "price": {"type": "number"},
                "description": {"type": "string"},
                "available": {"type": "boolean", "description": "Default: true"}
            },
            "required": ["name", "category", "price"]
        }
    },
    {
        "name": "list_menu",
        "description": "List menu items, optionally filtered by category.",
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "Filter by category (optional)"}
            }
        }
    },
    {
        "name": "update_menu_item",
        "description": "Update a menu item's price, availability, or description.",
        "input_schema": {
            "type": "object",
            "properties": {
                "item_id": {"type": "string"},
                "price": {"type": "number"},
                "available": {"type": "boolean"},
                "description": {"type": "string"}
            },
            "required": ["item_id"]
        }
    },
    {
        "name": "update_inventory",
        "description": "Set or update a restaurant inventory item's quantity and reorder level.",
        "input_schema": {
            "type": "object",
            "properties": {
                "item_name": {"type": "string"},
                "quantity": {"type": "number"},
                "unit": {"type": "string", "description": "e.g. lbs, cases, gallons"},
                "reorder_level": {"type": "number", "description": "Quantity at which to reorder"}
            },
            "required": ["item_name", "quantity", "unit"]
        }
    },
    {
        "name": "get_low_inventory",
        "description": "Get restaurant inventory items that are at or below their reorder level.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "add_reservation",
        "description": "Book a restaurant reservation.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Guest name"},
                "date": {"type": "string", "description": "YYYY-MM-DD"},
                "time": {"type": "string"},
                "party_size": {"type": "integer"},
                "phone": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["name", "date", "time", "party_size"]
        }
    },
    {
        "name": "list_reservations",
        "description": "List restaurant reservations for a specific date or all upcoming.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date": {"type": "string", "description": "YYYY-MM-DD for a specific date, or omit for all upcoming"}
            }
        }
    },
    {
        "name": "add_supplier",
        "description": "Add a restaurant supplier contact.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "category": {"type": "string", "description": "e.g. produce, meat, beverages, dry goods"},
                "contact": {"type": "string", "description": "Contact person name"},
                "phone": {"type": "string"},
                "email": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["name", "category", "contact"]
        }
    },

    # ── I005: RETAIL PRO ───────────────────────────────────────────────────
    {
        "name": "add_retail_product",
        "description": "Add a product to the retail store inventory.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "sku": {"type": "string", "description": "Unique SKU code"},
                "price": {"type": "number"},
                "category": {"type": "string"},
                "stock": {"type": "integer", "description": "Initial stock quantity"},
                "reorder_level": {"type": "integer", "description": "Reorder when stock hits this level. Default: 5"},
                "notes": {"type": "string"}
            },
            "required": ["name", "sku", "price", "category"]
        }
    },
    {
        "name": "list_products",
        "description": "List retail products, optionally filtered by category or low stock.",
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string"},
                "low_stock_only": {"type": "boolean", "description": "Show only items at or below reorder level"}
            }
        }
    },
    {
        "name": "update_stock",
        "description": "Adjust a product's stock quantity (use positive to add, negative to subtract).",
        "input_schema": {
            "type": "object",
            "properties": {
                "sku": {"type": "string"},
                "quantity_change": {"type": "integer", "description": "Positive to add stock, negative to remove"},
                "reason": {"type": "string", "description": "e.g. received shipment, damaged, correction"}
            },
            "required": ["sku", "quantity_change"]
        }
    },
    {
        "name": "get_low_stock",
        "description": "Get all retail products that are at or below their reorder level.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "add_retail_customer",
        "description": "Add a retail customer.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "email": {"type": "string"},
                "phone": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["name"]
        }
    },
    {
        "name": "record_sale",
        "description": "Record a retail sale. Automatically deducts from inventory.",
        "input_schema": {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "sku": {"type": "string"},
                            "qty": {"type": "integer"},
                            "price": {"type": "number"}
                        }
                    },
                    "description": "List of items sold: [{sku, qty, price}]"
                },
                "customer_name": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["items"]
        }
    },
    {
        "name": "get_sales_report",
        "description": "Get a retail sales report for the last N days.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "Number of days to include. Default: 30"}
            }
        }
    },
    {
        "name": "add_retail_supplier",
        "description": "Add a retail supplier.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "category": {"type": "string"},
                "contact": {"type": "string"},
                "phone": {"type": "string"},
                "email": {"type": "string"}
            },
            "required": ["name", "category", "contact"]
        }
    },

    # ── I006: SALON PRO ────────────────────────────────────────────────────
    {
        "name": "add_salon_client",
        "description": "Add a salon or spa client.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "phone": {"type": "string"},
                "email": {"type": "string"},
                "birthday": {"type": "string", "description": "YYYY-MM-DD or MM-DD"},
                "notes": {"type": "string"},
                "allergies": {"type": "string", "description": "Any product allergies or sensitivities"}
            },
            "required": ["name", "phone"]
        }
    },
    {
        "name": "list_salon_clients",
        "description": "List salon clients, optionally searching by name or phone.",
        "input_schema": {
            "type": "object",
            "properties": {
                "search": {"type": "string"}
            }
        }
    },
    {
        "name": "add_salon_appointment",
        "description": "Book a salon or spa appointment.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"},
                "date": {"type": "string", "description": "YYYY-MM-DD"},
                "time": {"type": "string"},
                "service": {"type": "string", "description": "Service being booked"},
                "staff": {"type": "string", "description": "Staff member (optional)"},
                "duration_min": {"type": "integer", "description": "Duration in minutes, default 60"},
                "notes": {"type": "string"}
            },
            "required": ["client_name", "date", "time", "service"]
        }
    },
    {
        "name": "list_salon_appointments",
        "description": "List salon appointments, optionally filtered by date or staff member.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date": {"type": "string", "description": "YYYY-MM-DD for a specific date"},
                "staff": {"type": "string", "description": "Filter by staff member name"}
            }
        }
    },
    {
        "name": "add_service",
        "description": "Add a service to the salon/spa service menu.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "category": {"type": "string", "description": "e.g. Hair, Nails, Skin, Massage"},
                "price": {"type": "number"},
                "duration_min": {"type": "integer"},
                "description": {"type": "string"}
            },
            "required": ["name", "category", "price", "duration_min"]
        }
    },
    {
        "name": "log_client_visit",
        "description": "Log a completed client visit with services and total.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"},
                "date": {"type": "string", "description": "YYYY-MM-DD"},
                "services": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of services performed"
                },
                "total": {"type": "number", "description": "Total amount charged"},
                "staff": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["client_name", "date", "services", "total"]
        }
    },
    {
        "name": "get_client_history",
        "description": "Get full history for a salon client — visits, preferences, upcoming appointments.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"}
            },
            "required": ["client_name"]
        }
    },

    # ── I007: CONTRACTOR PRO ───────────────────────────────────────────────
    {
        "name": "add_job",
        "description": "Add a contractor job.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"},
                "address": {"type": "string"},
                "description": {"type": "string"},
                "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                "end_date": {"type": "string", "description": "YYYY-MM-DD"},
                "status": {"type": "string", "description": "active, completed, on_hold. Default: active"},
                "notes": {"type": "string"}
            },
            "required": ["client_name", "address", "description"]
        }
    },
    {
        "name": "list_jobs",
        "description": "List contractor jobs, optionally filtered by status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "description": "active, completed, on_hold"}
            }
        }
    },
    {
        "name": "update_job",
        "description": "Update a contractor job's status, notes, or end date.",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string"},
                "status": {"type": "string"},
                "notes": {"type": "string"},
                "end_date": {"type": "string", "description": "YYYY-MM-DD"}
            },
            "required": ["job_id"]
        }
    },
    {
        "name": "add_estimate",
        "description": "Add a cost estimate to a job with labor, materials, and markup.",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string"},
                "labor_cost": {"type": "number"},
                "materials_cost": {"type": "number"},
                "description": {"type": "string"},
                "markup_pct": {"type": "number", "description": "Markup percentage, default 20"}
            },
            "required": ["job_id", "labor_cost", "materials_cost"]
        }
    },
    {
        "name": "add_materials",
        "description": "Add a materials list to a contractor job.",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string"},
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "qty": {"type": "number"},
                            "unit": {"type": "string"},
                            "cost": {"type": "number"}
                        }
                    },
                    "description": "List of materials: [{name, qty, unit, cost}]"
                }
            },
            "required": ["job_id", "items"]
        }
    },
    {
        "name": "add_subcontractor",
        "description": "Add a subcontractor to your contact list.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "trade": {"type": "string", "description": "e.g. electrician, plumber, framer"},
                "phone": {"type": "string"},
                "email": {"type": "string"},
                "rate": {"type": "string", "description": "e.g. $45/hr, $2000/project"},
                "notes": {"type": "string"}
            },
            "required": ["name", "trade", "phone"]
        }
    },
    {
        "name": "create_invoice",
        "description": "Generate an invoice for a contractor job based on estimates and hours logged.",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string"},
                "due_date": {"type": "string", "description": "YYYY-MM-DD"},
                "notes": {"type": "string"}
            },
            "required": ["job_id"]
        }
    },
    {
        "name": "log_job_hours",
        "description": "Log labor hours for a contractor job.",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string"},
                "date": {"type": "string", "description": "YYYY-MM-DD"},
                "hours": {"type": "number"},
                "worker": {"type": "string", "description": "Worker name"},
                "description": {"type": "string"}
            },
            "required": ["job_id", "date", "hours", "worker"]
        }
    },
    {
        "name": "get_job_summary",
        "description": "Get full details for a contractor job — estimate, materials, hours, and invoice status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string"}
            },
            "required": ["job_id"]
        }
    },

    # ── I008: TRADE SPECIALTIES ────────────────────────────────────────────
    {
        "name": "add_trade_job",
        "description": "Add a trade job for plumbing, electrical, HVAC, flooring, or roofing.",
        "input_schema": {
            "type": "object",
            "properties": {
                "trade": {"type": "string", "description": "plumbing, electrical, hvac, flooring, roofing"},
                "client_name": {"type": "string"},
                "address": {"type": "string"},
                "description": {"type": "string"},
                "status": {"type": "string", "description": "active, completed, on_hold. Default: active"},
                "notes": {"type": "string"}
            },
            "required": ["trade", "client_name", "address", "description"]
        }
    },
    {
        "name": "list_trade_jobs",
        "description": "List trade jobs, optionally filtered by trade type.",
        "input_schema": {
            "type": "object",
            "properties": {
                "trade": {"type": "string", "description": "plumbing, electrical, hvac, flooring, roofing"}
            }
        }
    },
    {
        "name": "add_trade_note",
        "description": "Log a note on a trade job, with an optional trade code reference.",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string"},
                "note": {"type": "string"},
                "code": {"type": "string", "description": "Optional trade code, e.g. NEC 210.8, IPC 2012"}
            },
            "required": ["job_id", "note"]
        }
    },
    {
        "name": "get_common_codes",
        "description": "Get quick-reference codes and specs for a specific trade — pipe sizes, AWG, SEER ratings, etc.",
        "input_schema": {
            "type": "object",
            "properties": {
                "trade": {"type": "string", "description": "plumbing, electrical, hvac, flooring, or roofing"}
            },
            "required": ["trade"]
        }
    },
    {
        "name": "add_material_order",
        "description": "Track a material order for a trade job.",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string"},
                "supplier": {"type": "string"},
                "items": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of items ordered"
                },
                "order_date": {"type": "string", "description": "YYYY-MM-DD, defaults to today"},
                "status": {"type": "string", "description": "ordered, received, backordered. Default: ordered"}
            },
            "required": ["job_id", "supplier", "items"]
        }
    },
    {
        "name": "get_trade_summary",
        "description": "Get a summary for a specific trade — job count, recent activity, and pending material orders.",
        "input_schema": {
            "type": "object",
            "properties": {
                "trade": {"type": "string", "description": "plumbing, electrical, hvac, flooring, or roofing"}
            },
            "required": ["trade"]
        }
    },

    # ── I009: ACCOUNTING PRO ───────────────────────────────────────────────
    {
        "name": "add_accounting_client",
        "description": "Add a client to your accounting practice.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "business_name": {"type": "string"},
                "phone": {"type": "string"},
                "email": {"type": "string"},
                "tax_id": {"type": "string", "description": "EIN or SSN (stored locally only)"},
                "filing_type": {"type": "string", "description": "e.g. 1040, S-Corp, LLC, C-Corp"},
                "notes": {"type": "string"}
            },
            "required": ["name", "business_name", "phone"]
        }
    },
    {
        "name": "list_accounting_clients",
        "description": "List accounting clients, optionally searching by name.",
        "input_schema": {
            "type": "object",
            "properties": {
                "search": {"type": "string"}
            }
        }
    },
    {
        "name": "add_tax_deadline",
        "description": "Track a tax deadline for a client.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"},
                "deadline_type": {"type": "string", "description": "e.g. Q1 Estimated Tax, Annual Return, Extension"},
                "due_date": {"type": "string", "description": "YYYY-MM-DD"},
                "description": {"type": "string"},
                "status": {"type": "string", "description": "pending, filed, extended. Default: pending"}
            },
            "required": ["client_name", "deadline_type", "due_date"]
        }
    },
    {
        "name": "get_upcoming_tax_deadlines",
        "description": "Get all pending tax deadlines due within the next N days (default 30). Also shows overdue deadlines.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "Number of days to look ahead. Default: 30"}
            }
        }
    },
    {
        "name": "log_transaction",
        "description": "Log an income or expense transaction for a client.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"},
                "date": {"type": "string", "description": "YYYY-MM-DD"},
                "type": {"type": "string", "description": "income or expense"},
                "amount": {"type": "number"},
                "description": {"type": "string"},
                "category": {"type": "string", "description": "e.g. payroll, rent, supplies, revenue"}
            },
            "required": ["client_name", "date", "type", "amount", "description"]
        }
    },
    {
        "name": "get_client_financials",
        "description": "Get income, expenses, and net for an accounting client, optionally filtered by year.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"},
                "year": {"type": "string", "description": "4-digit year, e.g. 2025. Omit for all time."}
            },
            "required": ["client_name"]
        }
    },
    {
        "name": "add_document_ref",
        "description": "Track a document reference for a client (local reference only, no file upload).",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"},
                "doc_type": {"type": "string", "description": "e.g. W-2, 1099, K-1, Bank Statement, Tax Return"},
                "filename": {"type": "string", "description": "Filename or description"},
                "year": {"type": "string", "description": "Tax year, e.g. 2024"},
                "notes": {"type": "string"}
            },
            "required": ["client_name", "doc_type", "filename", "year"]
        }
    },
    {
        "name": "get_document_refs",
        "description": "List all document references on file for an accounting client.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"}
            },
            "required": ["client_name"]
        }
    },

    # ── I010: HR PROFESSIONAL ──────────────────────────────────────────────
    {
        "name": "add_employee",
        "description": "Add an employee record.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "title": {"type": "string"},
                "department": {"type": "string"},
                "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                "email": {"type": "string"},
                "phone": {"type": "string"},
                "manager": {"type": "string"},
                "status": {"type": "string", "description": "active, inactive, terminated. Default: active"}
            },
            "required": ["name", "title", "department", "start_date"]
        }
    },
    {
        "name": "list_employees",
        "description": "List employees, optionally filtered by department or status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "department": {"type": "string"},
                "status": {"type": "string", "description": "active, inactive, terminated. Default: active"}
            }
        }
    },
    {
        "name": "get_employee",
        "description": "Get full details for an employee by name.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            },
            "required": ["name"]
        }
    },
    {
        "name": "add_onboarding_task",
        "description": "Add an onboarding task for a new employee.",
        "input_schema": {
            "type": "object",
            "properties": {
                "employee_name": {"type": "string"},
                "task": {"type": "string"},
                "due_date": {"type": "string", "description": "YYYY-MM-DD"},
                "completed": {"type": "boolean", "description": "Default: false"}
            },
            "required": ["employee_name", "task"]
        }
    },
    {
        "name": "get_onboarding_status",
        "description": "Get all onboarding tasks and completion status for an employee.",
        "input_schema": {
            "type": "object",
            "properties": {
                "employee_name": {"type": "string"}
            },
            "required": ["employee_name"]
        }
    },
    {
        "name": "log_pto_request",
        "description": "Log a PTO request for an employee.",
        "input_schema": {
            "type": "object",
            "properties": {
                "employee_name": {"type": "string"},
                "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                "end_date": {"type": "string", "description": "YYYY-MM-DD"},
                "type": {"type": "string", "description": "vacation, sick, or personal"},
                "status": {"type": "string", "description": "pending, approved, denied. Default: pending"},
                "notes": {"type": "string"}
            },
            "required": ["employee_name", "start_date", "end_date", "type"]
        }
    },
    {
        "name": "get_pto_balance",
        "description": "Get an employee's PTO balance — days allotted, used, and remaining for the current year.",
        "input_schema": {
            "type": "object",
            "properties": {
                "employee_name": {"type": "string"}
            },
            "required": ["employee_name"]
        }
    },
    {
        "name": "schedule_performance_review",
        "description": "Schedule a performance review for an employee.",
        "input_schema": {
            "type": "object",
            "properties": {
                "employee_name": {"type": "string"},
                "review_date": {"type": "string", "description": "YYYY-MM-DD"},
                "reviewer": {"type": "string", "description": "Name of reviewing manager"},
                "notes": {"type": "string"}
            },
            "required": ["employee_name", "review_date"]
        }
    },
    {
        "name": "get_upcoming_reviews",
        "description": "Get performance reviews scheduled in the next N days (default 60). Also shows overdue reviews.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "Days to look ahead. Default: 60"}
            }
        }
    },

    # ── I011: THERAPY PRO ──────────────────────────────────────────────────
    {
        "name": "add_therapy_client",
        "description": "Add a therapy or counseling client. All data stored locally only — HIPAA-conscious.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "dob": {"type": "string", "description": "YYYY-MM-DD"},
                "phone": {"type": "string"},
                "email": {"type": "string"},
                "insurance": {"type": "string"},
                "presenting_issue": {"type": "string", "description": "Brief description of presenting concern"},
                "notes": {"type": "string"}
            },
            "required": ["name"]
        }
    },
    {
        "name": "list_therapy_clients",
        "description": "List therapy clients, optionally searching by name.",
        "input_schema": {
            "type": "object",
            "properties": {
                "search": {"type": "string"}
            }
        }
    },
    {
        "name": "add_session_note",
        "description": "Add a session note for a therapy client. Stored locally only.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"},
                "date": {"type": "string", "description": "YYYY-MM-DD"},
                "duration_min": {"type": "integer", "description": "Session length in minutes"},
                "session_type": {"type": "string", "description": "individual, family, or group"},
                "note": {"type": "string", "description": "Session note content"},
                "mood_rating": {"type": "integer", "description": "Client mood rating 1-10 (optional)"},
                "provider": {"type": "string"}
            },
            "required": ["client_name", "date", "duration_min", "session_type", "note"]
        }
    },
    {
        "name": "get_session_history",
        "description": "Get recent session notes for a therapy client.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"},
                "limit": {"type": "integer", "description": "Max sessions to return. Default: 10"}
            },
            "required": ["client_name"]
        }
    },
    {
        "name": "add_therapy_appointment",
        "description": "Schedule a therapy or counseling appointment.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"},
                "date": {"type": "string", "description": "YYYY-MM-DD"},
                "time": {"type": "string"},
                "type": {"type": "string", "description": "individual, family, group, intake"},
                "provider": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["client_name", "date", "time", "type"]
        }
    },
    {
        "name": "list_therapy_appointments",
        "description": "List therapy appointments, optionally filtered by date.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date": {"type": "string", "description": "YYYY-MM-DD for a specific date"},
                "upcoming_only": {"type": "boolean", "description": "Show only future appointments (default true)"}
            }
        }
    },
    {
        "name": "add_treatment_plan",
        "description": "Save or update a treatment plan for a therapy client.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"},
                "goals": {"type": "string", "description": "Treatment goals"},
                "interventions": {"type": "string", "description": "Planned interventions and modalities"},
                "review_date": {"type": "string", "description": "YYYY-MM-DD — when to review the plan"},
                "provider": {"type": "string"}
            },
            "required": ["client_name", "goals", "interventions", "review_date"]
        }
    },
    {
        "name": "get_treatment_plan",
        "description": "Get the current treatment plan for a therapy client.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"}
            },
            "required": ["client_name"]
        }
    },

    # ── PROPERTY MANAGEMENT ────────────────────────────────────────────────
    {
        "name": "add_property",
        "description": "Add a rental property to the portfolio.",
        "input_schema": {
            "type": "object",
            "properties": {
                "address":   {"type": "string"},
                "type":      {"type": "string", "description": "e.g. single-family, condo, apartment, commercial"},
                "bedrooms":  {"type": "integer"},
                "bathrooms": {"type": "number"},
                "rent":      {"type": "number", "description": "Monthly rent amount"},
                "notes":     {"type": "string"}
            },
            "required": ["address", "type"]
        }
    },
    {
        "name": "list_properties",
        "description": "List rental properties, optionally filtered by status (vacant/occupied).",
        "input_schema": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "description": "vacant, occupied, or empty for all"}
            },
            "required": []
        }
    },
    {
        "name": "add_tenant",
        "description": "Add a tenant record.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name":    {"type": "string"},
                "phone":   {"type": "string"},
                "email":   {"type": "string"},
                "id_type": {"type": "string", "description": "Type of ID provided"},
                "notes":   {"type": "string"}
            },
            "required": ["name", "phone"]
        }
    },
    {
        "name": "list_tenants",
        "description": "List tenants, optionally searching by name.",
        "input_schema": {
            "type": "object",
            "properties": {
                "search": {"type": "string"}
            },
            "required": []
        }
    },
    {
        "name": "add_lease",
        "description": "Create a lease linking a tenant to a property.",
        "input_schema": {
            "type": "object",
            "properties": {
                "property_id":   {"type": "string"},
                "tenant_name":   {"type": "string"},
                "start_date":    {"type": "string"},
                "end_date":      {"type": "string"},
                "monthly_rent":  {"type": "number"},
                "deposit":       {"type": "number"},
                "notes":         {"type": "string"}
            },
            "required": ["property_id", "tenant_name", "start_date", "end_date", "monthly_rent"]
        }
    },
    {
        "name": "get_lease",
        "description": "Get the active lease for a tenant or property.",
        "input_schema": {
            "type": "object",
            "properties": {
                "tenant_name": {"type": "string"},
                "property_id": {"type": "string"}
            },
            "required": []
        }
    },
    {
        "name": "log_rent_payment",
        "description": "Record a rent payment from a tenant.",
        "input_schema": {
            "type": "object",
            "properties": {
                "tenant_name":   {"type": "string"},
                "amount":        {"type": "number"},
                "payment_date":  {"type": "string"},
                "method":        {"type": "string", "description": "check, cash, bank transfer, Venmo, etc."},
                "notes":         {"type": "string"}
            },
            "required": ["tenant_name", "amount", "payment_date"]
        }
    },
    {
        "name": "get_rent_status",
        "description": "Check rent payment status for all tenants or a specific tenant this month.",
        "input_schema": {
            "type": "object",
            "properties": {
                "tenant_name": {"type": "string"}
            },
            "required": []
        }
    },
    {
        "name": "add_maintenance_request",
        "description": "Log a maintenance or repair request for a property.",
        "input_schema": {
            "type": "object",
            "properties": {
                "property_id":  {"type": "string"},
                "tenant_name":  {"type": "string"},
                "description":  {"type": "string"},
                "priority":     {"type": "string", "description": "urgent, normal, or low"},
                "category":     {"type": "string", "description": "e.g. plumbing, electrical, HVAC, appliance"}
            },
            "required": ["property_id", "tenant_name", "description"]
        }
    },
    {
        "name": "list_maintenance_requests",
        "description": "List maintenance requests, filtered by status and/or property.",
        "input_schema": {
            "type": "object",
            "properties": {
                "status":      {"type": "string", "description": "open, in-progress, or completed"},
                "property_id": {"type": "string"}
            },
            "required": []
        }
    },
    {
        "name": "update_maintenance_request",
        "description": "Update the status or details of a maintenance request.",
        "input_schema": {
            "type": "object",
            "properties": {
                "request_id": {"type": "string"},
                "status":     {"type": "string", "description": "open, in-progress, or completed"},
                "notes":      {"type": "string"},
                "vendor":     {"type": "string"},
                "cost":       {"type": "number"}
            },
            "required": ["request_id"]
        }
    },

    # ── BUSINESS PRO ───────────────────────────────────────────────────────
    {
        "name": "add_business_customer",
        "description": "Add a customer or client to Business Pro CRM.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name":    {"type": "string"},
                "phone":   {"type": "string"},
                "email":   {"type": "string"},
                "company": {"type": "string"},
                "source":  {"type": "string", "description": "How they found you"},
                "notes":   {"type": "string"}
            },
            "required": ["name"]
        }
    },
    {
        "name": "list_business_customers",
        "description": "List business customers/clients, optionally filtered by search.",
        "input_schema": {
            "type": "object",
            "properties": {
                "search": {"type": "string"}
            },
            "required": []
        }
    },
    {
        "name": "get_business_customer",
        "description": "Get full details for a specific business customer.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            },
            "required": ["name"]
        }
    },
    {
        "name": "add_business_note",
        "description": "Add a note to a business customer record.",
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_name": {"type": "string"},
                "note":          {"type": "string"}
            },
            "required": ["customer_name", "note"]
        }
    },
    {
        "name": "create_business_invoice",
        "description": "Create an invoice for a Business Pro customer.",
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_name": {"type": "string"},
                "amount":        {"type": "number"},
                "description":   {"type": "string"},
                "due_date":      {"type": "string"},
                "notes":         {"type": "string"}
            },
            "required": ["customer_name", "amount", "description"]
        }
    },
    {
        "name": "list_invoices",
        "description": "List invoices, filtered by status or customer.",
        "input_schema": {
            "type": "object",
            "properties": {
                "status":        {"type": "string", "description": "unpaid, paid, overdue"},
                "customer_name": {"type": "string"}
            },
            "required": []
        }
    },
    {
        "name": "mark_invoice_paid",
        "description": "Mark a business invoice as paid.",
        "input_schema": {
            "type": "object",
            "properties": {
                "invoice_id":    {"type": "string"},
                "payment_date":  {"type": "string"},
                "method":        {"type": "string"}
            },
            "required": ["invoice_id"]
        }
    },
    {
        "name": "create_quote",
        "description": "Create a quote or estimate for a business customer.",
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_name": {"type": "string"},
                "amount":        {"type": "number"},
                "description":   {"type": "string"},
                "valid_days":    {"type": "integer", "description": "Days quote is valid"},
                "notes":         {"type": "string"}
            },
            "required": ["customer_name", "amount", "description"]
        }
    },
    {
        "name": "log_business_expense",
        "description": "Log a business expense (Business Pro). For personal household expenses use log_expense.",
        "input_schema": {
            "type": "object",
            "properties": {
                "amount":      {"type": "number"},
                "category":    {"type": "string"},
                "description": {"type": "string"},
                "date":        {"type": "string"},
                "vendor":      {"type": "string"}
            },
            "required": ["amount", "category", "description"]
        }
    },
    {
        "name": "get_expense_summary",
        "description": "Get a business expense summary for a month.",
        "input_schema": {
            "type": "object",
            "properties": {
                "month": {"type": "string", "description": "YYYY-MM format, blank = current month"}
            },
            "required": []
        }
    },
    {
        "name": "add_business_task",
        "description": "Add a business task or action item.",
        "input_schema": {
            "type": "object",
            "properties": {
                "task":        {"type": "string"},
                "due_date":    {"type": "string"},
                "assigned_to": {"type": "string"},
                "priority":    {"type": "string", "description": "urgent, normal, or low"}
            },
            "required": ["task"]
        }
    },
    {
        "name": "list_business_tasks",
        "description": "List open business tasks.",
        "input_schema": {
            "type": "object",
            "properties": {
                "assigned_to": {"type": "string"},
                "priority":    {"type": "string"}
            },
            "required": []
        }
    },
    {
        "name": "complete_business_task",
        "description": "Mark a business task as complete.",
        "input_schema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"}
            },
            "required": ["task_id"]
        }
    },
    {
        "name": "get_business_dashboard",
        "description": "Get a business overview dashboard: open invoices, recent customers, tasks, expenses.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },

    # ── INVENTORY PRO ──────────────────────────────────────────────────────
    {
        "name": "setup_inventory_config",
        "description": "Configure Inventory Pro for the business. Set business name, default location, currency, low-stock threshold, and tracking method.",
        "input_schema": {
            "type": "object",
            "properties": {
                "business_name":     {"type": "string"},
                "default_location":  {"type": "string", "description": "e.g. Main Warehouse, Store Floor"},
                "currency":          {"type": "string", "description": "3-letter code, e.g. USD"},
                "low_stock_default": {"type": "integer", "description": "Units threshold for low-stock alerts"},
                "track_by":          {"type": "string", "description": "sku or barcode"},
                "notes":             {"type": "string"}
            },
            "required": ["business_name"]
        }
    },
    {
        "name": "add_inventory_item",
        "description": "Add a new product or item to inventory.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name":          {"type": "string"},
                "sku":           {"type": "string"},
                "category":      {"type": "string"},
                "unit":          {"type": "string", "description": "each, case, lb, oz, etc."},
                "cost":          {"type": "number", "description": "Cost per unit"},
                "price":         {"type": "number", "description": "Retail/sell price per unit"},
                "reorder_level": {"type": "integer", "description": "Reorder when stock hits this level"},
                "location":      {"type": "string"},
                "notes":         {"type": "string"}
            },
            "required": ["name", "sku"]
        }
    },
    {
        "name": "list_inventory",
        "description": "List inventory items, optionally filtered by category, location, or low-stock flag.",
        "input_schema": {
            "type": "object",
            "properties": {
                "category":       {"type": "string"},
                "location":       {"type": "string"},
                "low_stock_only": {"type": "boolean"},
                "limit":          {"type": "integer"}
            },
            "required": []
        }
    },
    {
        "name": "search_inventory",
        "description": "Search inventory by name, SKU, category, or notes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "update_stock_level",
        "description": "Adjust stock for a single SKU. Use positive quantity_change for receiving stock, negative for sales or shrinkage.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sku":             {"type": "string"},
                "quantity_change": {"type": "integer"},
                "location":        {"type": "string"},
                "reason":          {"type": "string", "description": "sale, received, damaged, adjustment, etc."},
                "reference":       {"type": "string", "description": "PO number, order number, etc."}
            },
            "required": ["sku", "quantity_change"]
        }
    },
    {
        "name": "bulk_update_stock",
        "description": "Update stock for multiple SKUs at once. Pass a list of {sku, quantity_change, location, reason}.",
        "input_schema": {
            "type": "object",
            "properties": {
                "updates": {
                    "type": "array",
                    "description": "List of stock changes",
                    "items": {
                        "type": "object",
                        "properties": {
                            "sku":             {"type": "string"},
                            "quantity_change": {"type": "integer"},
                            "location":        {"type": "string"},
                            "reason":          {"type": "string"}
                        },
                        "required": ["sku", "quantity_change"]
                    }
                }
            },
            "required": ["updates"]
        }
    },
    {
        "name": "add_inventory_location",
        "description": "Add a storage location (warehouse, store room, shelf zone, etc.).",
        "input_schema": {
            "type": "object",
            "properties": {
                "name":        {"type": "string"},
                "description": {"type": "string"},
                "address":     {"type": "string"}
            },
            "required": ["name"]
        }
    },
    {
        "name": "list_inventory_locations",
        "description": "List all configured inventory storage locations.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "add_inventory_supplier",
        "description": "Add a supplier or vendor for inventory items.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name":      {"type": "string"},
                "contact":   {"type": "string"},
                "phone":     {"type": "string"},
                "email":     {"type": "string"},
                "lead_days": {"type": "integer", "description": "Typical lead time in days"},
                "notes":     {"type": "string"}
            },
            "required": ["name"]
        }
    },
    {
        "name": "create_purchase_order",
        "description": "Create a purchase order to a supplier for inventory items.",
        "input_schema": {
            "type": "object",
            "properties": {
                "supplier_name": {"type": "string"},
                "items": {
                    "type": "array",
                    "description": "List of {sku, quantity, unit_cost}",
                    "items": {
                        "type": "object",
                        "properties": {
                            "sku":       {"type": "string"},
                            "quantity":  {"type": "integer"},
                            "unit_cost": {"type": "number"}
                        },
                        "required": ["sku", "quantity"]
                    }
                },
                "notes": {"type": "string"}
            },
            "required": ["supplier_name", "items"]
        }
    },
    {
        "name": "receive_purchase_order",
        "description": "Mark a purchase order as received and automatically add stock. Optionally specify partial receipt.",
        "input_schema": {
            "type": "object",
            "properties": {
                "po_id": {"type": "string", "description": "PO id or PO number (e.g. PO-00001)"},
                "received_items": {
                    "type": "array",
                    "description": "Partial receipt: list of {sku, quantity}. Omit to receive entire PO.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "sku":      {"type": "string"},
                            "quantity": {"type": "integer"}
                        }
                    }
                }
            },
            "required": ["po_id"]
        }
    },
    {
        "name": "list_purchase_orders",
        "description": "List purchase orders, optionally filtered by status (ordered, received).",
        "input_schema": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "description": "ordered or received"}
            },
            "required": []
        }
    },
    {
        "name": "get_low_stock_report",
        "description": "Get a report of all items at or below their reorder level.",
        "input_schema": {
            "type": "object",
            "properties": {
                "location": {"type": "string", "description": "Filter by specific location, or blank for all"}
            },
            "required": []
        }
    },
    {
        "name": "get_inventory_value_report",
        "description": "Get total inventory value at cost and retail, with top items breakdown.",
        "input_schema": {
            "type": "object",
            "properties": {
                "location": {"type": "string"}
            },
            "required": []
        }
    },
    {
        "name": "get_stock_movement_report",
        "description": "Get a stock movement report showing units received and removed over a time period.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sku":  {"type": "string", "description": "Filter to one SKU, or blank for all"},
                "days": {"type": "integer", "description": "Number of days to look back (default 30)"}
            },
            "required": []
        }
    },
    {
        "name": "add_inventory_category",
        "description": "Add an inventory category (e.g. Electronics, Apparel, Frozen Foods).",
        "input_schema": {
            "type": "object",
            "properties": {
                "name":            {"type": "string"},
                "parent_category": {"type": "string", "description": "Parent category name for nested categories"},
                "notes":           {"type": "string"}
            },
            "required": ["name"]
        }
    },
]
