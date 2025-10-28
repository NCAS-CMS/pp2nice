from importlib import resources
import yaml, json

def load_vocab(name):
    vocab_dir = resources.files('pp2nice.assets.vocabs')
    path = vocab_dir.joinpath(name)
    with path.open("r") as f:
        if name.endswith((".yaml", ".yml")):
            return yaml.safe_load(f)
        elif name.endswith(".json"):
            return json.load(f)
        else:
            raise ValueError(f"Unsupported format: {name}")

example_cfg = yaml.safe_load(resources.files("pp2nice.assets").
                            joinpath('eg_config.yaml').
                            read_text())