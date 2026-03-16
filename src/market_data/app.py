from src.market_data.extractor.yahoo import yahoo_main

"""
Step 3 — Risk Management (Most Important)

This is where 90% fail.

You do NOT:

Risk more than 1–2% of total capital per trade

Move stops lower

Double down

Revenge trade

If account = £20k

Risk 1% per trade = £200

If stop is 5% away:

Position size = £200 / 0.05 = £4,000 position

This keeps you alive long term.

2️⃣ Entry timing (this is key)
Best swing entry times:

Mid-morning (after first 30–60 mins)

Late afternoon (last 60–90 mins)

Avoid:

First 5–15 mins (too chaotic)

Random lunchtime entries
"""


def main():
    yahoo_main()


if __name__ == "__main__":
    main()
