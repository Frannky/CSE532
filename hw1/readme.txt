1b.For the first question, the dosage unit column in my table is a decimal value instead of a float and in that case I did not convert it to an integer later. So the result I am getting is 4,281,954,926 instead of 4,281,954,931 and that's the reason why I make this difference.

1c.Since the values I used for group by are transaction_date and buyer_zip, so I only set index of these two values.


