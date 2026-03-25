using FotP.Engine.Criteria;
using FotP.Engine.Tiles.Abilities;

namespace FotP.Engine.Tiles
{
    /// <summary>
    /// Creates tile instances with their abilities and criteria.
    /// </summary>
    public static class TileFactory
    {
        public static Tile CreateTile(string name, int level, TileColor color)
        {
            var tile = new Tile(name, level, color);

            switch (name)
            {
                // ── Level 3 ─────────────────────────────────────────────────────

                case "Farmer":
                    tile.ClaimCriteria = new NOfAKind(2);
                    tile.AddAbility(new FarmerAbility());
                    break;
                case "Guard":
                    tile.ClaimCriteria = new NOfAKind(2);
                    tile.AddAbility(new GuardAbility());
                    break;
                case "Indentured Worker":
                    tile.ClaimCriteria = new NOfAKind(2);
                    tile.AddAbility(new IndenturedWorkerAbility());
                    break;
                case "Serf":
                    tile.ClaimCriteria = new NOfAKind(2);
                    tile.AddAbility(new SerfAbility());
                    break;
                case "Worker":
                    tile.ClaimCriteria = new NOfAKind(2);
                    tile.AddAbility(new WorkerAbility());
                    break;
                case "Beggar":
                    tile.ClaimCriteria = new NOfAKind(2);
                    tile.AddAbility(new BeggarAbility());
                    break;
                case "Servant":
                    tile.ClaimCriteria = new NOfAKind(2);
                    tile.AddAbility(new ServantAbility());
                    break;
                case "Herder":
                    // No ClaimCriteria: can be claimed with any locked dice
                    tile.AddAbility(new HerderAbility());
                    break;

                case "Soothsayer":
                    tile.ClaimCriteria = new Straight(3);
                    tile.AddAbility(new SoothsayerAbility());
                    break;

                case "Ankh":
                    tile.ClaimCriteria = new NOfAKind(2);
                    tile.AddAbility(new AnkhAbility());
                    break;
                case "Ancestral Guidance":
                    tile.ClaimCriteria = new NOfAKind(2);
                    tile.AddAbility(new AncestralGuidanceAbility());
                    break;
                case "Omen":
                    tile.ClaimCriteria = new NOfAKind(2);
                    tile.AddAbility(new OmenAbility());
                    break;

                // ── Level 4 ─────────────────────────────────────────────────────

                case "Artisan":
                    tile.ClaimCriteria = new NOfAKind(3);
                    tile.AddAbility(new ArtisanAbility());
                    break;
                case "Builder":
                    tile.ClaimCriteria = new NOfAKind(3);
                    tile.AddAbility(new BuilderAbility());
                    break;
                case "Noble Adoption":
                    tile.ClaimCriteria = new Straight(3);
                    tile.AddAbility(new NobleAdoptionAbility());
                    break;
                case "Palace Servants":
                    tile.ClaimCriteria = new NOfAKind(3);
                    tile.AddAbility(new PalaceServantsAbility());
                    break;
                case "Soldier":
                    tile.ClaimCriteria = new NOfAKind(3);
                    tile.AddAbility(new SoldierAbility());
                    break;
                case "Grain Merchant":
                    tile.ClaimCriteria = new TwoPairs();
                    tile.AddAbility(new GrainMerchantAbility());
                    break;

                case "Entertainer":
                    tile.ClaimCriteria = new NOfAKind(3);
                    tile.AddAbility(new EntertainerAbility());
                    break;
                case "Match Maker":
                    tile.ClaimCriteria = new Straight(3);
                    tile.AddAbility(new MatchMakerAbility());
                    break;

                case "Good Omen":
                    tile.ClaimCriteria = new NOfAKind(3);
                    tile.AddAbility(new GoodOmenAbility());
                    break;
                case "Palace Key":
                    tile.ClaimCriteria = new NOfAKind(3);
                    tile.AddAbility(new PalaceKeyAbility());
                    break;
                case "Spirit of the Dead":
                    tile.ClaimCriteria = new NOfAKind(3);
                    tile.AddAbility(new SpiritOfTheDeadAbility());
                    break;

                // ── Level 5 ─────────────────────────────────────────────────────

                case "Charioteer":
                    tile.ClaimCriteria = new NOfAKind(4);
                    tile.AddAbility(new CharioteerAbility());
                    break;
                case "Conspirator":
                    tile.ClaimCriteria = new Straight(4);
                    tile.AddAbility(new ConspiratorAbility());
                    break;
                case "Overseer":
                    tile.ClaimCriteria = new FullHouse();
                    tile.AddAbility(new OverseerAbility());
                    break;
                case "Ship Captain":
                    tile.ClaimCriteria = new NOfAKind(4);
                    tile.AddAbility(new ShipCaptainAbility());
                    break;
                case "Master Artisan":
                    tile.ClaimCriteria = new NOfAKind(4);
                    tile.AddAbility(new MasterArtisanAbility());
                    break;

                case "Tomb Builder":
                    tile.ClaimCriteria = new NOfAKind(4);
                    tile.AddAbility(new TombBuilderAbility());
                    break;
                case "Head Servant":
                    tile.ClaimCriteria = new Straight(4);
                    tile.AddAbility(new HeadServantAbility());
                    break;
                case "Priest":
                    tile.ClaimCriteria = new FullHouse();
                    tile.AddAbility(new PriestAbility());
                    break;

                case "Bad Omen":
                    tile.ClaimCriteria = new NOfAKind(4);
                    tile.AddAbility(new BadOmenAbility());
                    break;
                case "Burial Mask":
                    tile.ClaimCriteria = new NOfAKind(4);
                    tile.AddAbility(new BurialMaskAbility());
                    break;
                case "Royal Decree":
                    tile.ClaimCriteria = new Straight(4);
                    tile.AddAbility(new RoyalDecreeAbility());
                    break;

                // ── Level 6 ─────────────────────────────────────────────────────

                case "Embalmer":
                    tile.ClaimCriteria = new NOfAKind(5);
                    tile.AddAbility(new EmbalmerAbility());
                    break;
                case "Estate Overseer":
                    tile.ClaimCriteria = new ThreePairs();
                    tile.AddAbility(new EstateOverseerStartAbility());
                    tile.AddAbility(new EstateOverseerEndAbility());
                    break;
                case "Royal Attendants":
                    tile.ClaimCriteria = new NOfAKind(5);
                    tile.AddAbility(new RoyalAttendantsAbility());
                    break;
                case "Priestess":
                    tile.ClaimCriteria = new Straight(5);
                    tile.AddAbility(new PriestessAbility());
                    break;

                case "Grain Trader":
                    tile.ClaimCriteria = new NOfAKind(5);
                    tile.AddAbility(new GrainTraderAbility());
                    break;
                case "Priest of the Dead":
                    tile.ClaimCriteria = new ThreePairs();
                    tile.AddAbility(new PriestOfTheDeadAbility());
                    break;
                case "Astrologer":
                    tile.ClaimCriteria = new Straight(5);
                    tile.AddAbility(new AstrologerAbility());
                    break;
                case "Surveyor":
                    tile.ClaimCriteria = new SumGreaterEqual(20);
                    tile.AddAbility(new SurveyorAbility());
                    break;

                case "Pharaoh's Gift":
                    tile.ClaimCriteria = new NOfAKind(5);
                    tile.AddAbility(new PharaohsGiftAbility());
                    break;
                case "Secret Passage":
                    tile.ClaimCriteria = new ThreePairs();
                    tile.AddAbility(new SecretPassageAbility());
                    break;
                case "Treasure":
                    tile.ClaimCriteria = new SumGreaterEqual(18);
                    tile.AddAbility(new TreasureAbility());
                    break;

                // ── Level 7 ─────────────────────────────────────────────────────

                case "Queen":
                    tile.ClaimCriteria = new SumGreaterEqual(20);
                    tile.AddAbility(new QueenAbility());
                    break;
                case "General":
                    tile.ClaimCriteria = new Straight(5);
                    tile.AddAbility(new GeneralAbility());
                    break;
                case "Grand Vizier":
                    tile.ClaimCriteria = new NOfAKind(4);
                    tile.AddAbility(new GrandVizierAbility());
                    break;
                case "Granary Master":
                    tile.ClaimCriteria = new ThreePairs();
                    tile.AddAbility(new GranaryMasterStartAbility());
                    tile.AddAbility(new GranaryMasterEndAbility());
                    break;
                case "Heir":
                    tile.ClaimCriteria = new SumGreaterEqual(22);
                    tile.AddAbility(new HeirAbility());
                    break;

                case "Royal Astrologer":
                    tile.ClaimCriteria = new Straight(5);
                    tile.AddAbility(new RoyalAstrologerAbility());
                    break;
                case "Royal Mother":
                    tile.ClaimCriteria = new NOfAKind(4);
                    tile.AddAbility(new RoyalMotherAbility());
                    break;

                case "Queen's Favor":
                    tile.ClaimCriteria = new SumGreaterEqual(20);
                    tile.AddAbility(new QueensFavorAbility());
                    break;
                case "Royal Death":
                    tile.ClaimCriteria = new NOfAKind(4);
                    tile.AddAbility(new RoyalDeathAbility());
                    break;
                case "Royal Power":
                    tile.ClaimCriteria = new SumGreaterEqual(22);
                    tile.AddAbility(new RoyalPowerAbility());
                    break;

                default:
                    break;
            }

            return tile;
        }
    }
}
