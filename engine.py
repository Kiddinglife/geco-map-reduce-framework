class FakedMintedEngine(object):
    gamestates = [{"requestaction": "freespin", "winnings": 12, "stake": 15, "win":20},
                {"requestaction": "freespin", "winnings": 12, "stake": 15, "win":20},
                {"requestaction": "spin", "winnings": 12, "stake": 15, "win":20}]
    
    def play(self):
        gamestate = FakedMintedEngine.gamestates.pop()
        if gamestate != None:
            return gamestate;