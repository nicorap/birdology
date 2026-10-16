"""SU species parsed from DOF's Danish checklist (pdftotext -layout output)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.su_list import load_su_species, parse_checklist  # noqa: E402

EXCERPT = """\
DRC CATEGORY SCIENTIFIC NAME          DANISH NAME            ENGLISH NAME                     STATUS
  X         A        Oxyura leucocephala                     Hvidhovedet Skarveand                    White-headed Duck                       A WM
                     Oxyura jamaicensis                      Amerikansk Skarveand                     Ruddy Duck
  X        C5.2       O. j. jamaicensis                       Amerikansk Skarveand                     North American Ruddy Duck              A MB, R PM WM SM
            A        Cygnus olor                             Knopsvane                                Mute Swan                               FC RB, C PM WM
     A     Clangula hyemalis         Havlit                 Long-tailed Duck                 C PM WM, R SM
X    A     Polysticta stelleri       Stellersand            Steller's Eider                  R Ir PM WM, VR SM
           Otis tarda                Stortrappe             Great Bustard
X    A      O. t. tarda               Stortrappe             Western Great Bustard           A MB, A PM WM SM
           Streptopelia turtur       Turteldue              European Turtle Dove
     A      S. t. turtur              Turteldue              Northern European Turtle Dove   R MB, R PM, A WM            -1969
           Branta canadensis         Canadagås              Canada Goose
X    A      B. c. parvipes            Lille Canadagås        Lesser Canada Goose             A PM
     C1     B. c. canadensis          Atlantisk Canadagås    Atlantic Canada Goose           R Ir RB, C PM WM, S SM
(X)  A     Aquila clanga             Stor Skrigeørn         Greater Spotted Eagle            VR PM
           Lyrurus tetrix            Urfugl                 Black Grouse
     A      L. t. tetrix †            Urfugl                 Eurasian Black Grouse           E RB                        Extinct
APPENDIX 2: Category D
X    D     Anas formosa              Sibirisk Krikand       Baikal Teal                      A PM
"""


def test_species_marked_on_their_own_row_or_on_every_subspecies():
    names = [s for s, _, _ in parse_checklist(EXCERPT)]
    assert names == ["Oxyura leucocephala", "Oxyura jamaicensis", "Polysticta stelleri", "Otis tarda"]


def test_regional_marks_partial_subspecies_and_later_appendices_are_not_su():
    names = {s for s, _, _ in parse_checklist(EXCERPT)}
    assert "Aquila clanga" not in names        # (X): only from some regions
    assert "Branta canadensis" not in names    # one subspecies marked, the other not
    assert "Anas formosa" not in names         # Appendix 2 is not the Danish list
    assert "Cygnus olor" not in names


def test_names_carry_danish_and_english():
    assert ("Polysticta stelleri", "Stellersand", "Steller's Eider") in parse_checklist(EXCERPT)


def test_load_su_species(tmp_path):
    p = tmp_path / "su.csv"
    p.write_text("scientific_name,danish_name,english_name\nOtis tarda,Stortrappe,Great Bustard\n")
    assert load_su_species(p) == {"Otis tarda"}
