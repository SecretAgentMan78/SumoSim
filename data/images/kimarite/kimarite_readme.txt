Images from The Japan Times Sumo Special 2020: https://sports.japantimes.co.jp/sumo/techniques.html


DEV NOTES

Current design uses this matrix to determine likely kimarite

   (FightingStyle.OSHI, FightingStyle.OSHI): ["oshidashi", "tsukidashi", "hatakikomi"],
    (FightingStyle.OSHI, FightingStyle.YOTSU): ["oshidashi", "tsukiotoshi", "hikiotoshi"],
    (FightingStyle.OSHI, FightingStyle.HYBRID): ["oshidashi", "hatakikomi", "tsukidashi"],
    (FightingStyle.YOTSU, FightingStyle.OSHI): ["yorikiri", "uwatenage", "kotenage"],
    (FightingStyle.YOTSU, FightingStyle.YOTSU): ["yorikiri", "uwatenage", "shitatenage"],
    (FightingStyle.YOTSU, FightingStyle.HYBRID): ["yorikiri", "shitatenage", "uwatenage"],
    (FightingStyle.HYBRID, FightingStyle.OSHI): ["yorikiri", "hatakikomi", "oshidashi"],
    (FightingStyle.HYBRID, FightingStyle.YOTSU): ["hatakikomi", "yorikiri", "katasukashi"],
    (FightingStyle.HYBRID, FightingStyle.HYBRID): ["yorikiri", "oshidashi", "hatakikomi"],
	
Assumption: the model then selects a kimarite from one of the three based on the winning fighter's history; if no history, pick most likely historically broadly.

Issue: Highly limited list. There are over 80 official kimarite including "losing" techniques, 2 of which already happened in the 2026 Haru basho.
While the above list is good, it will never pick a really fun out of left field option. Those should still be INCREDIBLY rare in a typical simulation with
a signifcant number of runs, but they should come up from time to time.

Next updates to include the following:

1. In H2H solo matchups (one-offs), suggest a winning kimarite
2. Kimarite image to appear with the winner
3. Winning kimarite's overall popularity (using historical data) to show for a. the winning rikishi and b. the active sumo roster broadly
4. Ability to step through a tournament day by day without simulating all 15 days
5. Ability to project likely yusho winner on a graph per day
6. Use injury severity to predict individual fusensho or tournment kyujo likelihood
7. Include juryo roster to be able to fill in for kyujo rikiski in makuuchi
8. Use Haru 2026 data after its completion to better fit momentum, injury, and fatigue weights/sliders as a "default" configuration to start May basho with